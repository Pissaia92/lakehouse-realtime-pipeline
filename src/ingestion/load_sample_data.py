import json
from kafka import KafkaProducer
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

with open('/app/sample_orders.json', 'r') as f:
    data = json.load(f)

for item in data:
    producer.send('orders.orders_server.public.orders', value=item)
    print(f"Sent: {item}")
    time.sleep(0.5)

producer.flush()
print("✅ All data loaded into Kafka")