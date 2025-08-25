from kafka import KafkaProducer, KafkaAdminClient
from kafka.errors import NoBrokersAvailable
import json
import time

def test_kafka_connection():
    print("Testando conexão com Kafka...")
    
    bootstrap_servers_options = [
        'localhost:9092',
        'host.docker.internal:9092', 
        '127.0.0.1:9092'
    ]
    
    for bootstrap_servers in bootstrap_servers_options:
        try:
            print(f"Tentando: {bootstrap_servers}")
            
            # Teste com AdminClient primeiro
            admin = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                request_timeout_ms=10000
            )
            topics = admin.list_topics()
            print(f"✅ Conectado! Tópicos: {topics}")
            admin.close()
            
            # Agora teste o producer
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                request_timeout_ms=15000,
                retries=3
            )
            
            return producer, bootstrap_servers
            
        except NoBrokersAvailable:
            print(f"❌ Nenhum broker disponível em {bootstrap_servers}")
        except Exception as e:
            print(f"❌ Erro com {bootstrap_servers}: {e}")
    
    return None, None

# Teste a conexão
producer, successful_server = test_kafka_connection()

if producer:
    test_message = {
        "order_id": "test_123",
        "customer_id": "cust_456", 
        "total_amount": 100.50,
        "items": [{"product": "test", "quantity": 2}]
    }
    
    try:
        producer.send('orders-topic', test_message)
        producer.flush()
        print("✅ Mensagem enviada com sucesso!")
    except Exception as e:
        print(f"❌ Erro ao enviar mensagem: {e}")
else:
    print("⚠️  Nenhuma conexão funcionou.")