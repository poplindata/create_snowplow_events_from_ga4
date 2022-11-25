FROM python:3.8-slim
USER root

WORKDIR /app

COPY query_bq.py .
COPY requirements.txt .
COPY templates/ .
COPY static_files/ .

RUN pip install -r requirements.txt

ENV PORT 8080

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 query_bq:app