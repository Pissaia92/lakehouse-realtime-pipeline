FROM python:3.12-slim
RUN pip install duckdb
COPY server.py /app/server.py
WORKDIR /app
EXPOSE 8080
CMD ["python", "server.py"]