FROM apache/beam_python3.9_sdk:2.48.0
WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir -r requirements.txt



