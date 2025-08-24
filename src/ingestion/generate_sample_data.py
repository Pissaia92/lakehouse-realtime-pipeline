import random
import time
from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers='kafka:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

topic = 'orders.orders_server.public.orders'

while True:
  order_id = random.randint(1000, 9999)
  customer_id = random.randint(1, 100)
  amount = round(random.uniform(10, 500), 2)
  created_at = time.strftime('%Y-%m-%d %H:%M:%S')
  data = {
    "op": "c",
    "before": None,
    "after": {
      "order_id": order_id,
      "customer_id": customer_id,
      "amount": amount,
      "created_at": created_at
    }
  }
  producer.send(topic, value=data)
  print(f"Sent: {data['after']}")
  time.sleep(2)