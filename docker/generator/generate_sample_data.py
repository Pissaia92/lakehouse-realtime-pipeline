import random
import time
from confluent_kafka import Producer

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

producer = Producer({'bootstrap.servers': 'kafka:9092'})

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
    producer.produce(topic, value=str(data), callback=delivery_report)
    producer.poll(0)
    print(f"Sent: {data['after']}")
    time.sleep(2)

producer.flush()