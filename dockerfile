FROM python:3.9
RUN pip install "confluent-kafka[json,schemaregistry,rules]"
COPY config.py /app/config.py
COPY producer.py /app/producer.py
CMD ["python", "/app/producer.py"]