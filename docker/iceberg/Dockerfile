FROM python:3.12-slim

RUN pip install pyiceberg[hive] thrift  # ← ADICIONAR

COPY create_table.py /opt/iceberg/
WORKDIR /opt/iceberg

CMD ["python", "create_table.py"]