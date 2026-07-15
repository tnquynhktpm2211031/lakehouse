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

from env_config import MINIO_ENDPOINT
import os
import sys
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
from env_config import (
    MINIO_ACCESS_KEY, MINIO_SECRET_KEY,
    NESSIE_API_URL, HADOOP_HOME, SPARK_LOCAL_IP,
)

os.environ["HADOOP_HOME"]          = HADOOP_HOME
os.environ["PATH"]                 = os.path.join(HADOOP_HOME, "bin") + ";" + os.environ.get("PATH", "")
os.environ["AWS_ACCESS_KEY_ID"]    = MINIO_ACCESS_KEY
os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_SECRET_KEY
os.environ["SPARK_LOCAL_IP"]       = SPARK_LOCAL_IP
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
        .config("spark.driver.host", SPARK_LOCAL_IP) \
        .config("spark.driver.bindAddress", SPARK_LOCAL_IP) \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.lakehouse.uri", NESSIE_API_URL) \
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://university-lakehouse/iceberg-warehouse") \
        .config("spark.sql.catalog.lakehouse.cache-enabled", "false") \
        .config("spark.sql.catalogImplementation", "in-memory") \
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


def init_silver_table_if_needed(spark):
    """Đảm bảo namespace + bảng Silver tồn tại, và tự bổ sung cột mới
    (schema evolution) nếu bảng cũ đã tồn tại từ trước khi có các cột này."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            file_nguon STRING,
            ma_chi_tieu STRING,
            nhom_don_vi STRING,
            quy_danh_gia STRING,
            noi_dung_muc_tieu STRING,
            dinh_ky_thu_thap STRING,
            muc_dang_ky STRING,
            muc_dat STRING,
            ket_qua_he_thong STRING,
            nguyen_nhan STRING,
            hanh_dong_khac_phuc STRING,
            checksum_sha256 STRING,
            thoi_gian_ingest_silver TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (quy_danh_gia)
    """

    try:
        spark.sql(create_sql)
        print(f"✅ Đã đảm bảo bảng {SILVER_TABLE} tồn tại.")
    except Exception as exc:
        error_text = str(exc).lower()
        if any(token in error_text for token in ["notfoundexception", "metadata", "input stream", "no such file or directory"]):
            print(f"⚠️ Bảng {SILVER_TABLE} có metadata Iceberg bị hỏng hoặc thiếu. Đang tạo lại bảng từ đầu...")
            try:
                spark.sql(f"DROP TABLE IF EXISTS {SILVER_TABLE}")
            except Exception as drop_exc:
                print(f"⚠️ Không xoá được bảng cũ: {drop_exc}")
            spark.sql(create_sql)
            print(f"✅ Đã tạo lại bảng {SILVER_TABLE}.")
        else:
            raise

    # Bổ sung cột mới nếu bảng cũ đã tồn tại từ trước (schema evolution, không mất dữ liệu cũ)
    existing_columns = {f.name for f in spark.table(SILVER_TABLE).schema.fields}
    new_columns = {
        "noi_dung_muc_tieu": "STRING",
        "nguyen_nhan": "STRING",
        "hanh_dong_khac_phuc": "STRING",
    }
    for col_name, col_type in new_columns.items():
        if col_name not in existing_columns:
            print(f"🔧 Đang bổ sung cột '{col_name}' vào bảng {SILVER_TABLE}...")
            spark.sql(f"ALTER TABLE {SILVER_TABLE} ADD COLUMN {col_name} {col_type}")

def main():
    sys.stdout.reconfigure(encoding='utf-8')
    spark = get_spark_session()
    # Đọc TẤT CẢ các file parquet được sinh ra bởi ingest_bronze (các file có timestamp)
    bronze_parquet_path = "s3a://university-lakehouse/bronze/data_extracted_*.parquet"

    branch_name = make_branch_name("ingest_bronze_silver")

    try:
        # 1. Tạo branch mới từ main, chuyển context sang branch đó
        spark.catalog.clearCache()
        create_branch(spark, branch_name, from_ref="main")
        use_branch(spark, branch_name)

        # 2. Đảm bảo bảng tồn tại rồi MERGE dữ liệu -> mọi thay đổi chỉ nằm trên branch
        init_silver_table_if_needed(spark)

        try:
            df_bronze = spark.read.option("mergeSchema", "true").parquet(bronze_parquet_path)
        except Exception as e:
            print(f"Không tìm thấy dữ liệu Parquet tại {bronze_parquet_path}. Có thể chưa có file nào được ingest.")
            return

        # Đảm bảo source dataframe không chứa các dòng trùng lặp (nếu có nhiều file Parquet bị trùng)
        df_staging = df_bronze.dropDuplicates(["checksum_sha256"]).withColumn("thoi_gian_ingest_silver", current_timestamp())
        df_staging.createOrReplaceTempView("bronze_staging_view")

        spark.sql(f"""
            MERGE INTO {SILVER_TABLE} t
            USING bronze_staging_view s
            ON t.checksum_sha256 = s.checksum_sha256
            WHEN NOT MATCHED THEN
              INSERT (file_nguon, ma_chi_tieu, nhom_don_vi, quy_danh_gia, noi_dung_muc_tieu, dinh_ky_thu_thap, muc_dang_ky, muc_dat, ket_qua_he_thong, nguyen_nhan, hanh_dong_khac_phuc, checksum_sha256, thoi_gian_ingest_silver)
              VALUES (s.file_nguon, s.ma_chi_tieu, s.nhom_don_vi, s.quy_danh_gia, s.noi_dung_muc_tieu, s.dinh_ky_thu_thap, s.muc_dang_ky, s.muc_dat, s.ket_qua_he_thong, s.nguyen_nhan, s.hanh_dong_khac_phuc, s.checksum_sha256, s.thoi_gian_ingest_silver)
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
        print(f"DỮ LIỆU KHÔNG ĐẠT CHẤT LƯỢNG: {dqe}")
        print(f"Branch '{branch_name}' được giữ nguyên (không merge vào main) để kiểm tra thủ công.")
        print(f"Xem lại dữ liệu lỗi bằng: SELECT * FROM {SILVER_TABLE}@{branch_name}")

    except Exception as e:
        use_main(spark)
        print(f"Lỗi xử lý đường ống dữ liệu: {str(e)}")
        print(f"    Branch '{branch_name}' được giữ nguyên để kiểm tra.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()