FROM python:3.12-slim
COPY create_table.py /opt/iceberg/create_table.py
RUN pip install pyiceberg
CMD ["python3", "/opt/iceberg/create_table.py"]