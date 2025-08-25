import socket
import time
import os

def wait_for_kafka():
    print("⏳ Aguardando Kafka ficar pronto...")
    for i in range(30):  # 30 tentativas
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5)
            s.connect(('kafka', 9092))
            s.close()
            print("✅ Kafka pronto!")
            return True
        except:
            print(f"⏰ Tentativa {i+1}/30 - Kafka não pronto...")
            time.sleep(2)
    return False

if wait_for_kafka():
    os.system("python /app/generate_sample_data.py")
else:
    print("❌ Kafka não ficou pronto a tempo")