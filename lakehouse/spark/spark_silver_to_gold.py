# -*- coding: utf-8 -*-
"""
spark_silver_to_gold.py (Có Nessie Catalog Versioning)
------------------------------------------------------------
TẦNG GOLD (APACHE SPARK + ICEBERG) - ĐỀ TÀI DATA LAKEHOUSE CUSC

Áp dụng cùng quy trình version-control kiểu Git như Bronze->Silver:
  branch -> transform (overwrite 2 Data Mart) -> quality check
  -> merge vào main (nếu đạt) hoặc giữ branch (nếu lỗi)
------------------------------------------------------------
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, round, current_timestamp

from nessie_catalog_utils import (
    make_branch_name,
    create_branch,
    use_branch,
    use_main,
    merge_branch_to_main,
    check_quality_gold,
    DataQualityError,
)
from openmetadata_lineage_utils import get_client, push_lineage_safe

# --- CẤU HÌNH BIẾN MÔI TRƯỜNG WINDOWS ---
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")
os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"
# Bắt buộc trên Windows: tránh Spark bind theo hostname hệ thống gây lỗi
# Py4JNetworkError / ConnectionResetError ngay khi khởi tạo JavaSparkContext.
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,"
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,"
    "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
    "pyspark-shell"
)

GOLD_SUMMARY_TABLE = "lakehouse.gold.kpi_tong_hop_don_vi"
GOLD_DETAIL_TABLE = "lakehouse.gold.kpi_chi_tiet_dashboard"


def get_spark_session():
    print("⏳ Khởi tạo Spark Engine tính toán số liệu tầng Gold...")
    spark = (
        SparkSession.builder
        .appName("Silver_To_Gold_DataMart")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.lakehouse.uri", "http://localhost:19120/api/v1")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://university-lakehouse/iceberg-warehouse")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main():
    spark = get_spark_session()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

    branch_name = make_branch_name("silver_gold")

    try:
        # 1. Tạo branch mới từ main, chuyển context sang branch đó
        create_branch(spark, branch_name, from_ref="main")
        use_branch(spark, branch_name)

        # Bước 1: Đọc kho dữ liệu sạch Rich Schema từ tầng Silver (đọc từ main,
        # vì Silver đã được merge ổn định ở pipeline trước)
        print("📥 Đang đọc dữ liệu sạch từ lakehouse.silver.kpi_cusc_master...")
        df_silver = spark.read.table("lakehouse.silver.kpi_cusc_master")

        # ---------------------------------------------------------
        # DATA MART 1: TỔNG HỢP KPI THEO PHÒNG BAN
        # ---------------------------------------------------------
        print("⚙️ Nghiệp vụ 1: Tính toán tỷ lệ hoàn thành KPI nghiệp vụ của các đơn vị...")
        df_filtered = df_silver.filter(col("ket_qua_he_thong") != "CHƯA ĐẾN KỲ ĐÁNH GIÁ")

        df_summary = df_filtered.groupBy("quy_danh_gia", "nhom_don_vi").agg(
            count("*").alias("tong_chi_tieu_danh_gia"),
            count(when(col("ket_qua_he_thong") == "ĐẠT", True)).alias("so_chi_tieu_dat"),
            count(when(col("ket_qua_he_thong") == "KHÔNG ĐẠT", True)).alias("so_chi_tieu_khong_dat")
        )

        df_summary = df_summary.withColumn(
            "ty_le_hoan_thanh_phan_tram",
            round((col("so_chi_tieu_dat") / col("tong_chi_tieu_danh_gia")) * 100, 2)
        ).withColumn("thoi_gian_dong_goi_gold", current_timestamp())

        print("\n📊 1. PREVIEW DATA MART TỔNG HỢP PHÒNG BAN:")
        df_summary.orderBy(col("ty_le_hoan_thanh_phan_tram").desc()).show(truncate=False)

        # ---------------------------------------------------------
        # DATA MART 2: CHI TIẾT ĐẦY ĐỦ KPI
        # ---------------------------------------------------------
        print("⚙️ Nghiệp vụ 2: Đồng bộ danh sách Rich Schema phục vụ Pivot Table và bảng tra cứu...")
        df_detail = df_silver.select(
            "ma_chi_tieu",
            "nhom_don_vi",
            "quy_danh_gia",
            "dinh_ky_thu_thap",
            "muc_dang_ky",
            "muc_dat",
            "ket_qua_he_thong",
            "file_nguon"
        ).withColumn("thoi_gian_dong_goi_gold", current_timestamp())

        # ---------------------------------------------------------
        # GHI DỮ LIỆU LÊN BRANCH TẠM (chưa ảnh hưởng main)
        # ---------------------------------------------------------
        print(f"🧊 Đang ghi Data Mart Tổng hợp lên branch '{branch_name}'...")
        df_summary.write.format("iceberg").mode("overwrite").saveAsTable(GOLD_SUMMARY_TABLE)

        print(f"🧊 Đang ghi Data Mart Chi tiết lên branch '{branch_name}'...")
        df_detail.write.format("iceberg").mode("overwrite").saveAsTable(GOLD_DETAIL_TABLE)

        # 2. Data quality check TRÊN BRANCH trước khi merge
        check_quality_gold(spark, GOLD_SUMMARY_TABLE, GOLD_DETAIL_TABLE)

        # 3a. Đạt chất lượng -> merge branch vào main
        merge_branch_to_main(spark, branch_name)
        use_main(spark)

        print("\n🌟 HOÀN THÀNH TOÀN BỘ PIPELINE: BRONZE -> SILVER -> GOLD THÀNH CÔNG RỰC RỠ!")
        print(f"    Branch '{branch_name}' đã merge vào main và được giữ lại để audit.")

        # Đẩy lineage Silver -> Gold (2 bảng) lên OpenMetadata (best-effort)
        try:
            om_client = get_client()
            silver_fqn = "lakehouse-trino.lakehouse.silver.kpi_cusc_master"
            gold_summary_fqn = "lakehouse-trino.lakehouse.gold.kpi_tong_hop_don_vi"
            gold_detail_fqn = "lakehouse-trino.lakehouse.gold.kpi_chi_tiet_dashboard"

            push_lineage_safe(
                om_client, silver_fqn, gold_summary_fqn,
                sql_query=(
                    "INSERT OVERWRITE gold.kpi_tong_hop_don_vi "
                    "SELECT quy_danh_gia, nhom_don_vi, COUNT(*), "
                    "COUNT(CASE WHEN ket_qua_he_thong='ĐẠT' THEN 1 END), ... "
                    "FROM silver.kpi_cusc_master GROUP BY quy_danh_gia, nhom_don_vi"
                ),
                description="Tổng hợp tỷ lệ hoàn thành KPI theo đơn vị, chạy bởi spark_silver_to_gold.py",
            )
            push_lineage_safe(
                om_client, silver_fqn, gold_detail_fqn,
                sql_query=(
                    "INSERT OVERWRITE gold.kpi_chi_tiet_dashboard "
                    "SELECT ma_chi_tieu, nhom_don_vi, quy_danh_gia, dinh_ky_thu_thap, "
                    "muc_dang_ky, muc_dat, ket_qua_he_thong, file_nguon FROM silver.kpi_cusc_master"
                ),
                description="Đồng bộ bảng chi tiết phục vụ tra cứu, chạy bởi spark_silver_to_gold.py",
            )
        except Exception as e:
            print(f"⚠️  Không đẩy được lineage lên OpenMetadata (bỏ qua, không ảnh hưởng dữ liệu): {e}")

    except DataQualityError as dqe:
        # 3b. KHÔNG đạt chất lượng -> KHÔNG merge, giữ nguyên branch để debug
        use_main(spark)
        print(f"⚠️  DỮ LIỆU GOLD KHÔNG ĐẠT CHẤT LƯỢNG: {dqe}")
        print(f"⚠️  Branch '{branch_name}' được GIỮ NGUYÊN (không merge vào main) để kiểm tra thủ công.")
        print(f"    Xem lại dữ liệu lỗi bằng: SELECT * FROM {GOLD_SUMMARY_TABLE}@{branch_name}")

    except Exception as e:
        use_main(spark)
        print(f"❌ Thất bại ở tiến trình xử lý Gold: {str(e)}")
        print(f"    Branch '{branch_name}' được giữ nguyên để kiểm tra.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()