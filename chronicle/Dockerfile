FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY agent.py api.py otel_setup.py job_store.py stream_schemas.py judge_pipeline.py index.html ./
COPY mcp_servers/ ./mcp_servers/

EXPOSE 8000

CMD ["python", "api.py"]