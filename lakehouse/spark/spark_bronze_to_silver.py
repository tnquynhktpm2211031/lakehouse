# -*- coding: utf-8 -*-
"""
spark_bronze_to_silver.py (Có Nessie Catalog Versioning)
------------------------------------------------------------
TẦNG SILVER - GHI ĐẦY ĐỦ CỘT NGHIỆP VỤ VÀO APACHE ICEBERG

Quy trình version-control kiểu Git cho dữ liệu (qua Nessie):
  1. Tạo branch mới từ 'main'
  2. Chạy MERGE INTO trên branch đó (KHÔNG đụng tới 'main')
  3. Kiểm tra chất lượng dữ liệu (data quality check) trên branch
  4a. Đạt chất lượng  -> merge branch vào 'main'
  4b. Không đạt        -> giữ nguyên branch để debug, KHÔNG merge

Yêu cầu: file nessie_catalog_utils.py phải nằm cùng thư mục spark/
------------------------------------------------------------
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

from nessie_catalog_utils import (
    make_branch_name,
    create_branch,
    use_branch,
    use_main,
    merge_branch_to_main,
    check_quality_silver,
    DataQualityError,
)
from openmetadata_lineage_utils import get_client, ensure_bronze_table, push_lineage_safe

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")
os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"
# Bắt buộc trên Windows: nếu không set, Spark cố bind theo hostname hệ thống
# (thường không resolve được ra 127.0.0.1) khiến JVM gateway bị đóng ngay khi khởi tạo
# -> lỗi Py4JNetworkError / ConnectionResetError như đã gặp.
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,"
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,"
    "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
    "pyspark-shell"
)

SILVER_TABLE = "lakehouse.silver.kpi_cusc_master"


def get_spark_session():
    return SparkSession.builder \
        .appName("Bronze_To_Silver_Full_Schema") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.lakehouse.uri", "http://localhost:19120/api/v1") \
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://university-lakehouse/iceberg-warehouse") \
        .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()


def init_silver_table_if_needed(spark):
    """Chỉ tạo namespace + bảng nếu CHƯA tồn tại.
    QUAN TRỌNG: không còn DROP TABLE như bản cũ, vì DROP sẽ xoá luôn lịch sử
    version trong Nessie -> phá vỡ mục tiêu catalog versioning."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            file_nguon STRING,
            ma_chi_tieu STRING,
            nhom_don_vi STRING,
            quy_danh_gia STRING,
            dinh_ky_thu_thap STRING,
            muc_dang_ky STRING,
            muc_dat STRING,
            ket_qua_he_thong STRING,
            checksum_sha256 STRING,
            thoi_gian_ingest_silver TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (quy_danh_gia)
    """)


def main():
    spark = get_spark_session()
    bronze_parquet_path = "s3a://university-lakehouse/bronze/structured_data/data_extracted.parquet"

    branch_name = make_branch_name("ingest_bronze_silver")

    try:
        # 1. Tạo branch mới từ main, chuyển context sang branch đó
        create_branch(spark, branch_name, from_ref="main")
        use_branch(spark, branch_name)

        # 2. Đảm bảo bảng tồn tại rồi MERGE dữ liệu -> mọi thay đổi chỉ nằm trên branch
        init_silver_table_if_needed(spark)

        df_bronze = spark.read.parquet(bronze_parquet_path)
        df_staging = df_bronze.withColumn("thoi_gian_ingest_silver", current_timestamp())
        df_staging.createOrReplaceTempView("bronze_staging_view")

        spark.sql(f"""
            MERGE INTO {SILVER_TABLE} t
            USING bronze_staging_view s
            ON t.checksum_sha256 = s.checksum_sha256
            WHEN NOT MATCHED THEN
              INSERT (file_nguon, ma_chi_tieu, nhom_don_vi, quy_danh_gia, dinh_ky_thu_thap, muc_dang_ky, muc_dat, ket_qua_he_thong, checksum_sha256, thoi_gian_ingest_silver)
              VALUES (s.file_nguon, s.ma_chi_tieu, s.nhom_don_vi, s.quy_danh_gia, s.dinh_ky_thu_thap, s.muc_dang_ky, s.muc_dat, s.ket_qua_he_thong, s.checksum_sha256, s.thoi_gian_ingest_silver)
        """)
        print(f"✅ Đã ghi dữ liệu Rich Schema vào bảng Iceberg trên branch tạm thời '{branch_name}'.")

        # 3. Data quality check TRÊN BRANCH (chưa ảnh hưởng tới main)
        check_quality_silver(spark, SILVER_TABLE)

        # 4a. Đạt chất lượng -> merge branch vào main
        merge_branch_to_main(spark, branch_name)
        use_main(spark)

        print("\n📊 CHI TIẾT DỮ LIỆU CHUẨN HÓA TRONG BẢNG ICEBERG SILVER (main):")
        spark.sql(f"""
            SELECT ma_chi_tieu, nhom_don_vi, dinh_ky_thu_thap, muc_dang_ky, muc_dat, ket_qua_he_thong
            FROM {SILVER_TABLE}
            ORDER BY nhom_don_vi, ma_chi_tieu
        """).show(100, truncate=False)

        print(f"\n🌟 HOÀN THÀNH. Branch '{branch_name}' đã merge vào main và vẫn được giữ lại để audit.")

        # Đẩy lineage Bronze -> Silver lên OpenMetadata (best-effort, không làm sập pipeline nếu lỗi)
        try:
            om_client = get_client()
            bronze_fqn = ensure_bronze_table(om_client)
            silver_fqn = "lakehouse-trino.lakehouse.silver.kpi_cusc_master"
            push_lineage_safe(
                om_client, bronze_fqn, silver_fqn,
                sql_query=(
                    "MERGE INTO lakehouse.silver.kpi_cusc_master t "
                    "USING bronze_staging_view s ON t.checksum_sha256 = s.checksum_sha256 "
                    "WHEN NOT MATCHED THEN INSERT *"
                ),
                description="Nạp dữ liệu KPI đã trích xuất từ PDF/DOCX (Bronze) vào bảng Iceberg Silver, "
                             "chạy bởi spark_bronze_to_silver.py",
            )
        except Exception as e:
            print(f"⚠️  Không đẩy được lineage lên OpenMetadata (bỏ qua, không ảnh hưởng dữ liệu): {e}")

    except DataQualityError as dqe:
        # 4b. KHÔNG đạt chất lượng -> KHÔNG merge, giữ nguyên branch để debug
        use_main(spark)
        print(f"⚠️  DỮ LIỆU KHÔNG ĐẠT CHẤT LƯỢNG: {dqe}")
        print(f"⚠️  Branch '{branch_name}' được GIỮ NGUYÊN (không merge vào main) để kiểm tra thủ công.")
        print(f"    Xem lại dữ liệu lỗi bằng: SELECT * FROM {SILVER_TABLE}@{branch_name}")

    except Exception as e:
        use_main(spark)
        print(f"❌ Lỗi xử lý đường ống dữ liệu: {str(e)}")
        print(f"    Branch '{branch_name}' được giữ nguyên để kiểm tra.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()