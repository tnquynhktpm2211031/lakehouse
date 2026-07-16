# -*- coding: utf-8 -*-
"""
debug_duplicate_keys.py
------------------------------------------------------------
Soi các dòng bị trùng (ma_chi_tieu, quy_danh_gia) trên 1 branch Nessie
bằng SQL thuần. Dùng chính get_spark_session() giống các script pipeline
đang chạy được, để tránh lệch môi trường/venv.

Cách dùng:
    python debug_duplicate_keys.py ingest_bronze_silver_20260715_153404
------------------------------------------------------------
"""

import os
import sys
from pyspark.sql import SparkSession

from env_config import (
    MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_ENDPOINT,
    NESSIE_API_URL, HADOOP_HOME, SPARK_LOCAL_IP,
)

os.environ["HADOOP_HOME"]           = HADOOP_HOME
os.environ["PATH"]                  = os.path.join(HADOOP_HOME, "bin") + ";" + os.environ.get("PATH", "")
os.environ["AWS_ACCESS_KEY_ID"]     = MINIO_ACCESS_KEY
os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_SECRET_KEY
os.environ["SPARK_LOCAL_IP"]        = SPARK_LOCAL_IP
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,"
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,"
    "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
    "pyspark-shell"
)

SILVER_TABLE = "lakehouse.silver.kpi_cusc_master"


def get_spark_session():
    return SparkSession.builder \
        .appName("Debug_Duplicate_Keys") \
        .config("spark.driver.host", SPARK_LOCAL_IP) \
        .config("spark.driver.bindAddress", SPARK_LOCAL_IP) \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.lakehouse.uri", NESSIE_API_URL) \
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://university-lakehouse/iceberg-warehouse") \
        .config("spark.sql.catalog.lakehouse.s3.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()


def main():
    if len(sys.argv) < 2:
        print("Cách dùng: python debug_duplicate_keys.py <ten_branch>")
        sys.exit(1)

    branch_name = sys.argv[1]
    spark = get_spark_session()
    sys.stdout.reconfigure(encoding="utf-8")

    # Chuyển hẳn context sang branch cần soi (giống hệt cách use_branch() trong
    # nessie_catalog_utils.py đang dùng và đã chạy thành công) -> sau đó dùng
    # tên bảng BÌNH THƯỜNG, không cần @branch nữa.
    spark.sql(f"USE REFERENCE {branch_name} IN lakehouse")
    print(f"➡️  Đang đọc dữ liệu trên branch: '{branch_name}'\n")

    spark.sql(f"""
        SELECT ma_chi_tieu, quy_danh_gia, file_nguon, ket_qua_he_thong,
               muc_dat, muc_dat_numeric, thoi_gian_ingest_silver
        FROM {SILVER_TABLE}
        WHERE (ma_chi_tieu, quy_danh_gia) IN (
            SELECT ma_chi_tieu, quy_danh_gia
            FROM {SILVER_TABLE}
            GROUP BY ma_chi_tieu, quy_danh_gia
            HAVING COUNT(*) > 1
        )
        ORDER BY ma_chi_tieu, quy_danh_gia, thoi_gian_ingest_silver
    """).show(100, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()