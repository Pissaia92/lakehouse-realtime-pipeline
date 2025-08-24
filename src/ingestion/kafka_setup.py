import json
import requests
def create_topic():
  url = 'http://localhost:8083/connectors'
  data = {
    "name": "orders-source",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "tasks.max": "1",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "user",
      "database.password": "pass",
      "database.dbname": "orders_db",
      "database.server.name": "orders_server",
      "plugin.name": "pgoutput",
      "topic.prefix": "orders"
    }
  }
  response = requests.post(url, json=data)
  print(response.status_code)