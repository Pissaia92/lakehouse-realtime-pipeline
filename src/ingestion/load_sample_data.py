import pandas as pd
from kafka import KafkaProducer
import json
import time

# Espera o Kafka iniciar
print("Waiting for Kafka to be ready...")
while True:
    try:
        producer = KafkaProducer(bootstrap_servers='kafka:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        break
    except Exception as e:
        print(f"Kafka not ready yet: {e}")
        time.sleep(5)

print("Kafka is ready!")

df = pd.read_csv('/app/sample_orders.csv')

for _, row in df.iterrows():
    data = {
        "op": "c",
        "before": None,
        "after": {
            "order_id": int(row['order_id']),
            "customer_id": int(row['customer_id']),
            "amount": float(row['amount']),
            "created_at": row['created_at'],
            "status": row['status'],
            "region": row['region'],
            "payment_method": row['payment_method']
        }
    }
    producer.send('orders.orders_server.public.orders', value=data)
    print(f"Sent: {data['after']}")
    time.sleep(0.5)

producer.flush()
print("✅ All data loaded into Kafka")