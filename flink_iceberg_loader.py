from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy
import json
import requests

def create_iceberg_table():
    """Cria tabela Iceberg via REST API (se disponível)"""
    try:
        # Esta é uma abordagem alternativa - pode variar dependendo da versão
        response = requests.post(
            "http://localhost:8081/v1/sql/execute",
            json={
                "statement": """
                CREATE TABLE IF NOT EXISTS orders (
                    order_id INT,
                    customer_id INT,
                    amount DOUBLE,
                    status STRING,
                    region STRING,
                    payment_method STRING
                ) WITH (
                    'connector' = 'blackhole'
                )
                """
            }
        )
        print("Tabela criada:", response.status_code)
    except Exception as e:
        print("Erro ao criar tabela:", e)

def process_orders():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Adicionar JARs necessários
    env.add_jars(
        "file:///opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar",
        "file:///opt/flink/lib/iceberg-flink-runtime-1.18-1.5.0.jar"
    )
    
    # Configurar consumidor Kafka
    kafka_props = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'flink-consumer'
    }
    
    kafka_consumer = FlinkKafkaConsumer(
        'orders-topic',
        SimpleStringSchema(),
        kafka_props
    ).set_start_from_earliest()
    
    # Processar stream
    stream = env.add_source(kafka_consumer)
    
    def process_order(message):
        try:
            order = json.loads(message)
            print(f"Processed: Order {order['order_id']} - {order['amount']}")
            return order
        except Exception as e:
            print(f"Error: {e}")
            return None
    
    processed_stream = stream.map(process_order).filter(lambda x: x is not None)
    
    # Apenas exibir para teste
    processed_stream.print()
    
    env.execute("Order Processing Job")

if __name__ == "__main__":
    create_iceberg_table()
    process_orders()