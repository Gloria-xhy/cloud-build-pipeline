import base64
import json
import logging
from datetime import datetime, timezone
import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions, WorkerOptions, SetupOptions
from google.cloud import bigquery
from google.api_core.exceptions import Conflict, BadRequest
from apache_beam import Flatten

# 设置日志级别为 INFO
logging.basicConfig(level=logging.INFO)

# 全局变量，用于记录已创建的表和 schema 信息
SCHEMA_DICT = {}
CREATED_TABLES = set()


class SchemaManager:
    """管理 BigQuery schema 的类，负责生成和更新 schema"""
    def __init__(self, dataset_id, project_id):
        self.dataset_id = dataset_id
        self.project_id = project_id

    def load_existing_tables(self):
        """加载数据集下的所有表名和 schema"""
        global CREATED_TABLES, SCHEMA_DICT
        client = bigquery.Client(project=self.project_id)
        try:
            dataset_ref = client.dataset(self.dataset_id)
            tables = list(client.list_tables(dataset_ref))
            CREATED_TABLES = {table.table_id for table in tables}
            logging.info(f"Loaded existing tables: {CREATED_TABLES}")

            for table in tables:
                table_name = f"{client.project}.{self.dataset_id}.{table.table_id}"
                schema = self.get_table_schema(table_name)
                SCHEMA_DICT[table_name] = schema
                logging.info(f"Loaded schema for table {table_name}: {schema}")
        except Exception as e:
            logging.error(f"Error loading existing tables and schemas: {e}")

    def get_table_schema(self, table_name):
        """获取表的现有 schema，如果表不存在则返回空列表"""
        client = bigquery.Client()
        try:
            table_ref = client.get_table(table_name)
            return table_ref.schema
        except Exception as e:
            logging.warning(f"Table {table_name} does not exist or error occurred: {e}")
            return []

    def update_schema(self, table_name, new_fields):
        """更新 BigQuery 表的 schema"""
        client = bigquery.Client()
        try:
            alter_statements = [
                f"ALTER TABLE `{table_name}` ADD COLUMN IF NOT EXISTS {field.name} {field.field_type}"
                for field in new_fields
            ]
            for statement in alter_statements:
                query_job = client.query(statement)
                query_job.result()
                logging.info(f"Executed ALTER TABLE statement: {statement}")
        except Exception as e:
            logging.error(f"Error updating schema for {table_name}: {e}")

    def create_table(self, table_name, schema):
        """检查并在必要时创建 BigQuery 表"""
        global CREATED_TABLES
        client = bigquery.Client()
        if table_name in CREATED_TABLES:
            return

        table = bigquery.Table(table_name, schema=schema)
        try:
            client.create_table(table)
            logging.info(f"Created table {table_name} with schema: {schema}")
            CREATED_TABLES.add(table_name)
        except Conflict:
            logging.info(f"Table {table_name} already created by another worker.")

    def determine_bigquery_type(self, value, key):
        """根据值的类型确定 BigQuery 字段类型"""
        if isinstance(value, str):
            return bigquery.SchemaField(key, "STRING", mode="NULLABLE")
        elif value is None:
            return bigquery.SchemaField(key, "STRING", mode="NULLABLE")
        elif isinstance(value, bool):
            return bigquery.SchemaField(key, "BOOLEAN", mode="NULLABLE")
        elif isinstance(value, int):
            return bigquery.SchemaField(key, "INTEGER", mode="NULLABLE")
        elif isinstance(value, float):
            return bigquery.SchemaField(key, "FLOAT", mode="NULLABLE")
        elif isinstance(value, datetime):
            return bigquery.SchemaField(key, "TIMESTAMP", mode="NULLABLE")
        elif isinstance(value, (dict, list)):
            return bigquery.SchemaField(key, "STRING", mode="NULLABLE")
        else:
            return bigquery.SchemaField(key, "STRING", mode="NULLABLE")

class DecodeAndProcessMessage(beam.DoFn):
    """解码和初步处理 Pub/Sub 消息"""
    def process(self, element):
        try:
            logging.debug(f"Received Pub/Sub message element: {element}")
            decoded_message = element.decode("utf-8")
            event_data = json.loads(decoded_message)
            logging.debug(f"Decoded Pub/Sub message: {event_data}")

            event_id = event_data.get('eventId')
            event_group = event_data.get('eventGroup')
            properties_str = event_data.get('properties', '{}')
            properties = json.loads(properties_str)

            if not event_id or not event_group:
                logging.error("Missing required fields: eventId or eventGroup.")
                return

            uuid = properties.get("uuid")
            if not uuid:
                #logging.error("UUID is missing from properties, skipping insert.")
                return

            timestamp_id = properties.get("timestamp_id")
            if not timestamp_id:
                logging.error("timestamp_id is missing from properties.")
                return

            event_time = datetime.fromtimestamp(int(timestamp_id) / 1000, tz=timezone.utc).isoformat()

            row = {
                "eventid": event_id,
                "eventgroup": event_group,
                "event_time": event_time,
                "uuid": uuid
            }

            filtered_properties = {
                ('obus_' + k[1:] if k.startswith('$') else k): v
                for k, v in properties.items()
            }

            row.update(filtered_properties)

            transformed_row = {key.lower(): json.dumps(value) if isinstance(value, (dict, list)) else str(value) for key, value in row.items()}

            table_name = f"qpon-1c174.dataset_xhy_test.{event_group}_{event_id}_ProcessedMessage_dataflow"
            yield (table_name, transformed_row)
        except Exception as e:
            logging.error(f"Error processing message: {e}")


class CollectSchemaUpdates(beam.DoFn):
    """收集需要更新的 schema 信息"""
    def __init__(self, schema_manager):
        self.schema_manager = schema_manager

    def process(self, element):
        #logging.info(f"[element]: {element}")
        table_name, rows = element
        existing_schema = SCHEMA_DICT.get(table_name, [])
        current_fields = {field.name for field in existing_schema}
        new_fields = []

        for row in rows:
            for key, value in row.items():
                if key.lower() not in current_fields:
                    new_field = self.schema_manager.determine_bigquery_type(value, key.lower())
                    new_fields.append(new_field)
                    current_fields.add(key.lower())

        #logging.info(f"New fields: {new_fields}; Row data: {rows}")
        yield (table_name, new_fields)


class UpdateSchemaOnce(beam.DoFn):
    """统一新建表和更新 schema"""
    def __init__(self, schema_manager):
        self.schema_manager = schema_manager

    def process(self, element):
        table_name, new_fields = element
        latest_schema = self.schema_manager.get_table_schema(table_name)

        if not latest_schema:
            logging.info(f"Table {table_name} does not exist. Creating table.")
            initial_schema = new_fields
            self.schema_manager.create_table(table_name, schema=initial_schema)
            SCHEMA_DICT[table_name] = initial_schema
        else:
            logging.info(f"Table {table_name} exists. Checking for schema updates.")
            latest_fields = {field.name for field in latest_schema}
            fields_to_add = [field for field in new_fields if field.name not in latest_fields]

            if fields_to_add:
                logging.info(f"Updating schema for table {table_name} with fields: {fields_to_add}")
                self.schema_manager.update_schema(table_name, fields_to_add)

            SCHEMA_DICT[table_name] = latest_schema + fields_to_add

        yield table_name

class FlattenRows(beam.DoFn):
    """将元组中的行数据平铺成单独的行，忽略表名"""
    def process(self, element):
        _, rows = element  # 忽略表名，只处理行数据
        for row in rows:
            yield row  # 只返回行数据

def run(argv=None):
    options = PipelineOptions(argv)
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'qpon-1c174'
    google_cloud_options.region = 'asia-southeast2'
    google_cloud_options.job_name = f'xhy-dataflow-{datetime.now().strftime("%Y%m%d-%H%M%S")}'
    google_cloud_options.staging_location = 'gs://xhy_bucket/templates/staging'
    google_cloud_options.temp_location = 'gs://xhy_bucket/templates/temp'

    worker_options = options.view_as(WorkerOptions)
    worker_options.sdk_container_image = 'gcr.io/qpon-1c174/xhy_image:latest'
    worker_options.max_num_workers = 2  # 设置最大 worker 数量
    worker_options.autoscaling_algorithm = 'THROUGHPUT_BASED'  # 启用基于吞吐量的自动缩放

    options.view_as(StandardOptions).streaming = True
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    # 设置 save_main_session 选项
    setup_options = options.view_as(SetupOptions)
    setup_options.save_main_session = True

    schema_manager = SchemaManager(dataset_id='dataset_xhy_test', project_id='qpon-1c174')
    schema_manager.load_existing_tables()  # 初始化时加载现有表名
    def generate_schema_updated_flag():
        """生成一个表示 schema 更新完成的标志"""
        yield "SCHEMA_UPDATED"

    with beam.Pipeline(options=options) as p:
        # 1. 读取并解码数据
        messages = (
            p
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic='projects/qpon-1c174/topics/QponPagesTopic')
            | "Decode and Process Message" >> beam.ParDo(DecodeAndProcessMessage())
        )
        # 2. 分窗口并按表名分组gcloud services enable sourcerepo.googleapis.com
        grouped_messages = (
            messages
            | "Window into Fixed Windows" >> beam.WindowInto(beam.window.FixedWindows(300))
            | "Group by Table Name" >> beam.GroupByKey()
        )
        # 3. 收集需要更新的 schema 信息
        schema_updates = (
            grouped_messages
            | "Collect Schema Updates" >> beam.ParDo(CollectSchemaUpdates(schema_manager))
        )
        #4. 统一新建表和更新 schema
        updated_tables = (
            schema_updates
            | "Update Schema Once" >> beam.ParDo(UpdateSchemaOnce(schema_manager))
            | "Generate Schema Done Flag" >> beam.FlatMap(lambda _: generate_schema_updated_flag())
        )
        #5. 等待 schema 更新完成后再写入 BigQuery
        final_stream = (
            [grouped_messages, updated_tables]
            | "Wait for Schema Update" >> Flatten()
            | "Filter Messages Only" >> beam.Filter(lambda x: x != "SCHEMA_UPDATED")  # 过滤掉标志
        )

        # 6. 将元组中的行数据平铺
        flattened_messages = (
            final_stream
            | "Flatten Rows" >> beam.ParDo(FlattenRows())
        )
        # 7. 写入 BigQuery
        flattened_messages | "Write to BigQuery" >> WriteToBigQuery(
            table=lambda row: f"qpon-1c174.dataset_xhy_test.{row['eventgroup']}_{row['eventid']}_ProcessedMessage_dataflow",
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            write_disposition=BigQueryDisposition.WRITE_APPEND
        )


if __name__ == '__main__':
    run()