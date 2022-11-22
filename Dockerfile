FROM python:3.8-slim
USER root

WORKDIR /app

COPY query_bq.py .
COPY requirements.txt .

RUN pip install -r requirements.txt

CMD ["sh", "-c", "python query_bq.py"]