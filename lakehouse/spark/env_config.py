# -*- coding: utf-8 -*-
"""
env_config.py
------------------------------------------------------------
Module tập trung load biến môi trường cho toàn bộ Spark pipeline.
Ưu tiên: biến môi trường hệ thống > file .env > giá trị mặc định.

Cách dùng trong các script khác:
    from env_config import MINIO_ACCESS_KEY, MINIO_SECRET_KEY, ...
------------------------------------------------------------
"""

import os
from pathlib import Path

# Tìm file .env tại thư mục gốc dự án (2 cấp trên thư mục spark/)
_ROOT_DIR = Path(__file__).resolve().parent.parent.parent  # lakehouse/spark -> lakehouse -> root
_ENV_FILE = _ROOT_DIR / ".env"

# Load .env nếu python-dotenv có sẵn (optional dependency)
try:
    from dotenv import load_dotenv
    if _ENV_FILE.exists():
        load_dotenv(_ENV_FILE, override=False)  # override=False: ưu tiên biến hệ thống
except ImportError:
    pass  # Không có dotenv vẫn OK nếu biến đã set qua os.environ / setx


# ── MinIO ────────────────────────────────────────────────────
MINIO_ENDPOINT    = os.environ.get("MINIO_ENDPOINT",    "http://127.0.0.1:9000")
MINIO_ACCESS_KEY  = os.environ.get("MINIO_ACCESS_KEY",  "minioadmin")
MINIO_SECRET_KEY  = os.environ.get("MINIO_SECRET_KEY",  "minioadmin")
MINIO_BUCKET_NAME = os.environ.get("MINIO_BUCKET_NAME", "university-lakehouse")

# ── PostgreSQL ───────────────────────────────────────────────
PG_HOST           = os.environ.get("PG_HOST",           "127.0.0.1")
PG_PORT           = int(os.environ.get("PG_PORT",       "5432"))
PG_USER           = os.environ.get("PG_USER",           "postgres")
PG_PASSWORD       = os.environ.get("PG_PASSWORD",       "")
PG_MAINTENANCE_DB = os.environ.get("PG_MAINTENANCE_DB", "postgres")
NESSIE_DB         = os.environ.get("NESSIE_DB",         "nessie_db")

# ── Nessie ───────────────────────────────────────────────────
NESSIE_API_URL    = os.environ.get("NESSIE_API_URL",    "http://localhost:19120/api/v1")

# ── OpenMetadata ─────────────────────────────────────────────
OPENMETADATA_HOST_PORT  = os.environ.get("OPENMETADATA_HOST_PORT",  "http://localhost:8585/api")
OPENMETADATA_JWT_TOKEN  = os.environ.get("OPENMETADATA_JWT_TOKEN",  "")

# ── Spark / Hadoop (Windows) ──────────────────────────────────
HADOOP_HOME       = os.environ.get("HADOOP_HOME",       r"C:\hadoop")
SPARK_LOCAL_IP    = os.environ.get("SPARK_LOCAL_IP",    "127.0.0.1")
