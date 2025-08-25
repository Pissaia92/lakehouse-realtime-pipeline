from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'orders-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=5000  # ← Timeout de 5 segundos
)

print("📩 Consumer aguardando mensagens (5 segundos)...")
for message in consumer:
    print(f"Mensagem: {message.value}")

print("✅ Consumer finalizado!")