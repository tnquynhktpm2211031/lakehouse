import os

SECRET_KEY = os.getenv("SECRET_KEY", "nhuquynh_data_lakehouse_secret_key")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_HOURS = 2

# MinIO Config
MINIO_URL = os.getenv("MINIO_URL", "127.0.0.1:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "university-lakehouse")
