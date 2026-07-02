"""
spark_silver_to_gold.py
------------------------------------------------------------
Tổng hợp KPI đánh giá CUSC theo quý + nhóm đơn vị từ bảng Silver,
ghi kết quả vào bảng Iceberg lakehouse.gold.kpi_cusc_dashboard
(data mart sẵn sàng cho Superset/Trino query).

Yêu cầu chạy trước: python spark_bronze_to_silver.py

FIX so với bản gốc:
  - Nhãn "chưa đến kỳ" trong dữ liệu thực tế là "CHƯA ĐẾN KỲ ĐÁNH GIÁ"
    (do spark_ingest_bronze.py chuẩn hoá), nhưng bản gốc lại so sánh
    với "CHƯA ĐẾN KỲ" -> luôn ra 0. Đã sửa khớp đúng nhãn.
------------------------------------------------------------
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, round as spark_round

# ============================================================
# CẤU HÌNH MÔI TRƯỜNG WINDOWS & HADOOP
# ============================================================
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages "
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.104.5 "
    "pyspark-shell"
)

SILVER_TABLE = "lakehouse.silver.cusc_chat_luong"
GOLD_NAMESPACE = "lakehouse.gold"
GOLD_TABLE = "lakehouse.gold.kpi_cusc_dashboard"

# Phải khớp CHÍNH XÁC với nhãn được ghi ở spark_ingest_bronze.py
KETQUA_DAT = "ĐẠT"
KETQUA_CHUA_DEN_KY = "CHƯA ĐẾN KỲ ĐÁNH GIÁ"


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("CUSC-Silver-To-Gold-KPI")
        .master("local[*]")
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


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("\n[1/2] --- Đọc dữ liệu từ Silver Iceberg Table ---")
        df_chat_luong = spark.read.table(SILVER_TABLE)

        print("[2/2] --- Tổng hợp KPI Đánh giá CUSC theo Nhóm Đơn vị ---")
        df_kpi_cusc = (
            df_chat_luong.groupBy("quy_danh_gia", "nhom_don_vi")
            .agg(
                count("ma_chi_tieu").alias("tong_chi_tieu"),
                count(when(col("ket_qua") == KETQUA_DAT, 1)).alias("so_chi_tieu_dat"),
                count(when(col("ket_qua") == KETQUA_CHUA_DEN_KY, 1)).alias("so_chi_tieu_cho"),
            )
            .withColumn(
                "ty_le_hoan_thanh",
                spark_round((col("so_chi_tieu_dat") / col("tong_chi_tieu")) * 100, 2),
            )
        )

        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD_NAMESPACE}")
        print(f"[*] Đang ghi Data Mart: {GOLD_TABLE}...")
        df_kpi_cusc.writeTo(GOLD_TABLE).createOrReplace()

        print("\n✅ KẾT QUẢ TẦNG GOLD (SẴN SÀNG CHO SUPERSET):")
        spark.read.table(GOLD_TABLE).show(truncate=False)

    except Exception as ex:
        print(f"\n❌ LỖI HỆ THỐNG: {str(ex)}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()