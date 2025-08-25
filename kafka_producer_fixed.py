import csv
import json
import time
import os
import socket
from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime

# Configurações
KAFKA_BROKER = 'localhost:9092'  # Vamos usar localhost mesmo
TOPIC_NAME = 'orders-topic'
CSV_FILE_PATH = os.path.join('data', 'sample_orders.csv')

def test_kafka_connection():
    """Testa a conexão com o Kafka"""
    print("Testando conectividade com Kafka...")
    
    try:
        host, port = KAFKA_BROKER.split(':')
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, int(port)))
        if result == 0:
            print(f"✓ Porta {port} está aberta e acessível em {host}")
            return True
        else:
            print(f"✗ Porta {port} não está acessível em {host}")
            return False
    except Exception as e:
        print(f"Erro no teste de socket: {e}")
        return False
    finally:
        try:
            sock.close()
        except:
            pass

def create_producer():
    """Cria produtor Kafka"""
    try:
        print(f"Tentando conectar com: {KAFKA_BROKER}")
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(2, 0, 2),
            request_timeout_ms=30000,
            retry_backoff_ms=500,
            metadata_max_age_ms=30000
        )
        
        print(f"✓ Produtor Kafka criado com sucesso")
        return producer
        
    except Exception as e:
        print(f"✗ Erro ao criar produtor Kafka: {e}")
        return None

def send_orders_to_kafka():
    """Envia ordens para o Kafka"""
    
    # Testar conexão primeiro
    if not test_kafka_connection():
        print("Não foi possível conectar ao Kafka")
        return
    
    # Criar produtor
    producer = create_producer()
    if not producer:
        return
    
    try:
        with open(CSV_FILE_PATH, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            print(f"✓ Arquivo {CSV_FILE_PATH} aberto com sucesso")
            print("Iniciando envio de dados...")
            
            for row_num, row in enumerate(reader, 1):
                try:
                    # Converter dados
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
                    future = producer.send(TOPIC_NAME, order_data)
                    
                    # Opcional: esperar confirmação (pode remover se causar timeout)
                    # future.get(timeout=10)
                    
                    print(f"✓ Enviado pedido #{row_num}: ID {order_data['order_id']}")
                    time.sleep(0.5)
                    
                except Exception as e:
                    print(f"✗ Erro no pedido #{row_num}: {e}")
                    continue
        
        print("✓ Todos os dados foram enviados!")
        
    except FileNotFoundError:
        print(f"✗ Arquivo não encontrado: {CSV_FILE_PATH}")
        print("Verifique se o arquivo existe no caminho especificado")
    except Exception as e:
        print(f"✗ Erro durante o processamento: {e}")
    finally:
        try:
            producer.flush(timeout=10)
            producer.close()
            print("✓ Produtor fechado")
        except Exception as e:
            print(f"Erro ao fechar produtor: {e}")

if __name__ == "__main__":
    send_orders_to_kafka()