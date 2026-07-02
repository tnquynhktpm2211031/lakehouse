"""
spark_bronze_to_silver.py
------------------------------------------------------------
Đọc dữ liệu đã bóc tách từ PDF (bronze/structured_pdf_data/, do
spark_ingest_bronze.py tạo ra), làm sạch/mapping cột, rồi ghi vào
bảng Iceberg lakehouse.silver.cusc_chat_luong (qua Nessie catalog).

Yêu cầu chạy trước: python spark_ingest_bronze.py
------------------------------------------------------------
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp

# ============================================================
# CẤU HÌNH MÔI TRƯỜNG WINDOWS & HADOOP
# ============================================================
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

BRONZE_PATH = "s3a://university-lakehouse/bronze/unstructured_data"
SILVER_NAMESPACE = "lakehouse.silver"
SILVER_TABLE = "lakehouse.silver.cusc_chat_luong"


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("CUSC-Bronze-To-Silver-ETL")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
            "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.104.5",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.lakehouse.uri", "http://localhost:19120/api/v1")
        .config("spark.sql.catalog.lakehouse.ref", "main")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://university-lakehouse/iceberg-warehouse")
        .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.io.nativeio.NativeIO", "false")
        .getOrCreate()
    )


def ensure_silver_table(spark: SparkSession):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SILVER_NAMESPACE}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE}
        (
            ma_chi_tieu STRING,
            nhom_don_vi STRING,
            quy_danh_gia STRING,
            ket_qua STRING,
            nguon_du_lieu STRING,
            thoi_gian_cap_nhat TIMESTAMP
        )
        USING ICEBERG
    """)


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        ensure_silver_table(spark)

        print("⏳ Đang đọc dữ liệu bảng từ Bronze...")
        df_bronze = spark.read.parquet(BRONZE_PATH)

        df_silver = (
            df_bronze.withColumnRenamed("ket_qua_he_thong", "ket_qua")
            .withColumn("nguon_du_lieu", lit("PDF_Table_Extract"))
            .withColumn("thoi_gian_cap_nhat", current_timestamp())
            .select(
                "ma_chi_tieu",
                "nhom_don_vi",
                "quy_danh_gia",
                "ket_qua",
                "nguon_du_lieu",
                "thoi_gian_cap_nhat",
            )
        )

        print("✅ Đã chuẩn bị dữ liệu thành công! Xem trước dữ liệu:")
        df_silver.show(truncate=False)

        print("⏳ Đang ghi vào Iceberg Silver...")
        df_silver.writeTo(SILVER_TABLE).append()
        print("🌟 HOÀN TẤT GHI TẦNG SILVER!")

    except Exception as e:
        print(f"❌ Lỗi: {str(e)}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()