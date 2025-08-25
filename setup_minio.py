from minio import Minio

def setup_minio():
    client = Minio(
        "localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )
    
    # Criar bucket para Iceberg
    buckets = ["iceberg-warehouse"]
    for bucket in buckets:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            print(f"Bucket {bucket} criado com sucesso!")
        else:
            print(f"Bucket {bucket} já existe.")

if __name__ == "__main__":
    setup_minio()