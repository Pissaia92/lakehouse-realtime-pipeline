from kafka import KafkaProducer
import pandas as pd
import json
import time
import os

print("🚀 Iniciando generator com kafka-python...")

# Configurações - DEFINIR ANTES DE USAR
topic = "orders-topic"  # ← USAR O MESMO TÓPICO QUE SEU TESTE
file_path = "/data/sample_orders.csv"  # ← PATH CORRETO DENTRO DO CONTAINER

print(f"📡 Conectando ao Kafka: kafka:9092")
print(f"📂 Lendo arquivo: {file_path}")

try:
    # Criar producer APENAS UMA VEZ
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',  # ← CORRETO
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        request_timeout_ms=30000,
        retries=3  # ← ADICIONAR RETRIES
    )
    print("✅ Producer criado com sucesso!")
    
except Exception as e:
    print(f"❌ Erro ao criar producer: {e}")
    exit(1)

try:
    # Verificar se arquivo existe
    if not os.path.exists(file_path):
        print(f"❌ Arquivo não encontrado: {file_path}")
        print("📋 Listando diretório /data:")
        if os.path.exists('/data'):
            print(f"   Conteúdo de /data: {os.listdir('/data')}")
        else:
            print("   ❌ Diretório /data não existe")
        exit(1)
    
    # Carregar dados
    df = pd.read_csv(file_path)
    print(f"✅ CSV carregado: {len(df)} linhas, {len(df.columns)} colunas")
    print(f"📊 Colunas: {list(df.columns)}")
    
    # Enviar dados
    for i, row in df.iterrows():
        data = row.to_dict()
        producer.send(topic, value=data)
        
        if i % 10 == 0:  # Log a cada 10 linhas
            print(f"📤 Enviada linha {i}: {data}")
            
        time.sleep(0.1)  # Pequena pausa
    
    producer.flush()
    print(f"🎉 Todos os {len(df)} dados enviados para o tópico '{topic}'!")
    
except Exception as e:
    print(f"❌ Erro durante o processamento: {e}")
    import traceback
    traceback.print_exc()
finally:
    producer.close()