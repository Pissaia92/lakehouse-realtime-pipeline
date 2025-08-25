import csv
import json
import time
import os
from kafka import KafkaProducer
from datetime import datetime

# Configurações do Kafka
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'orders-topic'
CSV_FILE_PATH = os.path.join('data', 'sample_orders.csv')

# Inicializar o produtor Kafka
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(2, 0, 2)
    )
    print(f"Conectado ao Kafka em {KAFKA_BROKER}")
except Exception as e:
    print(f"Erro ao conectar com Kafka: {e}")
    exit(1)

def send_orders_to_kafka():
    try:
        with open(CSV_FILE_PATH, 'r') as file:
            reader = csv.DictReader(file)
            print(f"Iniciando envio de dados from {CSV_FILE_PATH}")
            
            for row_num, row in enumerate(reader, 1):
                # Converter para o formato desejado
                order_data = {
                    'order_id': int(row['order_id']),
                    'customer_id': int(row['customer_id']),
                    'amount': float(row['amount']),
                    'created_at': row['created_at'],
                    'status': row['status'],
                    'region': row['region'],
                    'payment_method': row['payment_method'],
                    'processing_time': datetime.now().isoformat()
                }
                
                # Enviar para o Kafka
                producer.send(TOPIC_NAME, order_data)
                print(f"Enviado pedido #{row_num}: ID {order_data['order_id']}")
                
                # Pequena pausa para simular dados em tempo real
                time.sleep(1)
        
        print("Todos os dados foram enviados com sucesso!")
        
    except FileNotFoundError:
        print(f"Arquivo não encontrado: {CSV_FILE_PATH}")
        print("Verifique se o caminho está correto")
    except Exception as e:
        print(f"Erro durante o processamento: {e}")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    send_orders_to_kafka()