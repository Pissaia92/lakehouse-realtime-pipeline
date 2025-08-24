from kafka import KafkaProducer
import pandas as pd
import json
import time
import os

print("🚀 Iniciando generator com kafka-python...")

producer = KafkaProducer(
    bootstrap_servers = "172.18.0.3:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    request_timeout_ms=30000,  # 30 segundos
    retry_backoff_ms=1000,
    metadata_max_age_ms=30000
)

# Configurações
topic = "orders"
bootstrap_servers = "kafka:9092"
file_path = "/app/data/sample_orders.csv"

print(f"📡 Conectando ao Kafka: {bootstrap_servers}")
print(f"📂 Lendo arquivo: {file_path}")

try:
    # Criar producer
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        request_timeout_ms=30000
    )
    print("✅ Producer criado com sucesso!")
    
except Exception as e:
    print(f"❌ Erro ao criar producer: {e}")
    exit(1)

try:
    # Carregar dados
    df = pd.read_csv(file_path)
    print(f"✅ CSV carregado: {len(df)} linhas")
    
    # Enviar dados
    for i, row in df.iterrows():
        data = row.to_dict()
        producer.send(topic, value=data)
        if i % 10 == 0:  # Log a cada 10 linhas
            print(f"📤 Enviada linha {i}: {data}")
        time.sleep(0.1)
    
    producer.flush()
    print("🎉 Todos os dados enviados com sucesso!")
    
except Exception as e:
    print(f"❌ Erro durante o processamento: {e}")