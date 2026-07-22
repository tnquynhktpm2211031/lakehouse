# -*- coding: utf-8 -*-
"""
spark_silver_to_gold.py (Có Nessie Catalog Versioning)
------------------------------------------------------------
TẦNG GOLD (APACHE SPARK + ICEBERG) - ĐỀ TÀI DATA LAKEHOUSE CUSC

[Yêu cầu 1] GOLD_*_COLUMNS: ép .select() theo thứ tự cột cố định trước khi
writeTo -> không còn lệch cột giữa các lần chạy job.

[Yêu cầu 3] PHONG_BAN_MAP: bảng tra tên đầy đủ phòng ban -> cột 'ten_phong_ban'.
Bảng Gold thứ 4 'dm_chi_tieu' đóng vai trò DATA DICTIONARY / CHÚ THÍCH tập
trung: mỗi mã chỉ tiêu -> thuộc phòng ban nào, nằm ở bảng/cột nguồn nào trong
Gold, kỳ đầu tiên/kỳ gần nhất xuất hiện. Add bảng này thành 1 Table chart đặt
ngay trên dashboard Superset để ai xem cũng thấy chú giải, không cần tra riêng.
LƯU Ý: tên phòng ban trong PHONG_BAN_MAP là suy luận từ nội dung báo cáo mẫu,
cần xác nhận lại với đơn vị trước khi dùng chính thức cho người dùng cuối.

[Yêu cầu 4] Bảng Gold thứ 3 'kpi_so_sanh_ky': so sánh cùng 1 ma_chi_tieu giữa
2 kỳ liên tiếp bằng LAG(), tính % tăng/giảm dựa trên muc_dat_numeric.
[FIX QUAN TRỌNG] Sắp xếp theo quy_danh_gia dạng STRING ("Q1/2026") bị SAI khi
vắt qua ranh giới năm, vì so sánh string: 'Q4/2025' > 'Q1/2026' (do ký tự '4' >
'1'), trong khi thực tế Q4/2025 xảy ra TRƯỚC Q1/2026. Đã thêm cột
'quy_danh_gia_sort_key' = năm*10 + quý (số nguyên) để ORDER BY/LAG/MIN/MAX
đúng theo thời gian thật, dùng cho cả bảng so sánh kỳ VÀ bảng data dictionary.

[FIX WINDOWS] quy_danh_gia_sort_key trước đây tính bằng UDF Python, hay gây
lỗi "Python worker exited unexpectedly (crashed)" trên Windows (do Spark phải
spawn tiến trình Python con để chạy UDF). Đã thay bằng regexp_extract() +
cast() thuần Spark SQL, chạy trong JVM, không còn spawn Python con nữa.
------------------------------------------------------------
"""

import os
import sys
from env_config import (
    MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_ENDPOINT,
    NESSIE_API_URL, HADOOP_HOME, SPARK_LOCAL_IP,
)

os.environ["HADOOP_HOME"]           = HADOOP_HOME
os.environ["PATH"]                  = os.path.join(HADOOP_HOME, "bin") + ";" + os.environ.get("PATH", "")
os.environ["AWS_ACCESS_KEY_ID"]     = MINIO_ACCESS_KEY
os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_SECRET_KEY
os.environ["SPARK_LOCAL_IP"] = SPARK_LOCAL_IP
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,"
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,"
    "org.apache.hadoop:hadoop-aws:3.3.4 "
    "pyspark-shell"
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, count, round, current_timestamp, lag, create_map, lit,
    first, min as spark_min, max as spark_max, struct, regexp_extract,
)
from pyspark.sql.window import Window
from itertools import chain

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

GOLD_SUMMARY_TABLE    = "lakehouse.gold.kpi_tong_hop_don_vi"
GOLD_DETAIL_TABLE     = "lakehouse.gold.kpi_chi_tiet_dashboard"
GOLD_COMPARISON_TABLE = "lakehouse.gold.kpi_so_sanh_ky"
GOLD_DICT_TABLE       = "lakehouse.gold.dm_chi_tieu"   # [MỚI - Yêu cầu 3] Data dictionary

# --- [Yêu cầu 1] Data dictionary cột: thứ tự CHUẨN, không đổi giữa các lần chạy ---
GOLD_SUMMARY_COLUMNS = [
    "quy_danh_gia", "nhom_don_vi",
    "tong_chi_tieu_danh_gia", "so_chi_tieu_dat", "so_chi_tieu_khong_dat",
    "ty_le_hoan_thanh_phan_tram", "thoi_gian_dong_goi_gold",
]
GOLD_DETAIL_COLUMNS = [
    "ma_chi_tieu", "nhom_don_vi", "ten_phong_ban", "quy_danh_gia",
    "noi_dung_muc_tieu", "dinh_ky_thu_thap",
    "muc_dang_ky", "muc_dat", "muc_dat_numeric", "ket_qua_he_thong",
    "nguyen_nhan", "hanh_dong_khac_phuc",
    "file_nguon", "thoi_gian_dong_goi_gold",
]
GOLD_COMPARISON_COLUMNS = [
    "ma_chi_tieu", "nhom_don_vi", "ten_phong_ban",
    "quy_danh_gia", "muc_dat_numeric",
    "quy_danh_gia_ky_truoc", "muc_dat_numeric_ky_truoc",
    "tang_truong_phan_tram", "thoi_gian_dong_goi_gold",
]
# [MỚI - Yêu cầu 3] Cột của bảng "chú thích / data dictionary"
GOLD_DICT_COLUMNS = [
    "ma_chi_tieu", "nhom_don_vi", "ten_phong_ban",
    "noi_dung_muc_tieu",
    "ky_dau_tien_xuat_hien", "ky_gan_nhat_cap_nhat",
    "nguon_bang_chi_tiet", "nguon_bang_tong_hop", "nguon_bang_so_sanh_ky",
    "cot_khoa_join", "thoi_gian_dong_goi_gold",
]

# --- [Yêu cầu 3] Bảng tra tên đầy đủ phòng ban theo mã nhom_don_vi ---
# LƯU Ý: suy luận từ nội dung báo cáo mẫu BM08.09.L, cần xác nhận lại với đơn vị.
PHONG_BAN_MAP = {
    "ĐT":   "Phòng Đào tạo",
    "PM":   "Trung tâm Phần mềm (mảng dự án/phát triển phần mềm)",
    "QTCL": "Bộ phận Quản trị Chất lượng",
    "VP":   "Văn phòng",
    "RD":   "Phòng Nghiên cứu & Phát triển (R&D)",
    "HT":   "Phòng Hạ tầng - An ninh mạng (QTANM)",
}


def add_quy_danh_gia_sort_key(df, col_name="quy_danh_gia"):
    """'Q1/2026' -> cột quy_danh_gia_sort_key = 20261 (năm*10 + quý), tính
    bằng Spark SQL native (regexp_extract + cast), KHÔNG dùng UDF Python để
    tránh lỗi 'Python worker exited unexpectedly' hay gặp trên Windows.
    Không khớp pattern -> regexp_extract trả '' -> cast int ra NULL (giống
    hành vi hàm Python cũ khi không match)."""
    quy = regexp_extract(col(col_name), r"Q(\d)/(\d{4})", 1).cast("int")
    nam = regexp_extract(col(col_name), r"Q(\d)/(\d{4})", 2).cast("int")
    return df.withColumn("quy_danh_gia_sort_key", nam * 10 + quy)


def get_spark_session():
    print("Khoi tao Spark Engine tinh toan so lieu tang Gold...")
    spark = (
        SparkSession.builder
        .appName("Silver_To_Gold_DataMart")
        .config("spark.driver.host", SPARK_LOCAL_IP)
        .config("spark.driver.bindAddress", SPARK_LOCAL_IP)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.lakehouse.uri", NESSIE_API_URL)
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://university-lakehouse/iceberg-warehouse")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def with_ten_phong_ban(df):
    """Join thêm cột 'ten_phong_ban' dựa trên PHONG_BAN_MAP (mã -> tên đầy đủ)."""
    mapping_expr = create_map([lit(x) for x in chain(*PHONG_BAN_MAP.items())])
    return df.withColumn("ten_phong_ban", mapping_expr[col("nhom_don_vi")])


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    spark = get_spark_session()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

    branch_name = make_branch_name("silver_gold")

    try:
        create_branch(spark, branch_name, from_ref="main")
        use_branch(spark, branch_name)

        print("Đang đọc dữ liệu sạch từ lakehouse.silver.kpi_cusc_master...")
        try:
            df_silver = spark.read.table("lakehouse.silver.kpi_cusc_master")
        except Exception:
            print("Không tìm thấy bảng Silver (lakehouse.silver.kpi_cusc_master). Có thể chưa có dữ liệu ở tầng Silver.")
            return

        # Cột sort key dùng chung cho window ordering & min/max theo đúng thời gian thật
        df_silver = add_quy_danh_gia_sort_key(df_silver)

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

        df_summary = df_summary.select(*GOLD_SUMMARY_COLUMNS)  # [Yêu cầu 1]

        print("\n1. PREVIEW DATA MART TỔNG HỢP PHÒNG BAN:")
        df_summary.orderBy(col("nhom_don_vi").asc()).show(truncate=False)

        # ---------------------------------------------------------
        # DATA MART 2: CHI TIẾT ĐẦY ĐỦ KPI + tên phòng ban
        # ---------------------------------------------------------
        print("⚙️ Nghiệp vụ 2: Đồng bộ danh sách Rich Schema phục vụ Pivot Table và bảng tra cứu...")
        df_detail = df_silver.select(
            "ma_chi_tieu", "nhom_don_vi", "quy_danh_gia",
            "noi_dung_muc_tieu", "dinh_ky_thu_thap",
            "muc_dang_ky", "muc_dat", "muc_dat_numeric", "ket_qua_he_thong",
            "nguyen_nhan", "hanh_dong_khac_phuc", "file_nguon"
        ).withColumn("thoi_gian_dong_goi_gold", current_timestamp())

        df_detail = with_ten_phong_ban(df_detail)          # [Yêu cầu 3]
        df_detail = df_detail.select(*GOLD_DETAIL_COLUMNS)  # [Yêu cầu 1]

        # ---------------------------------------------------------
        # DATA MART 3: SO SÁNH GIỮA CÁC KỲ CỦA CÙNG 1 MÃ CHỈ TIÊU  [Yêu cầu 4]
        # ---------------------------------------------------------
        print("⚙️ Nghiệp vụ 3: Tính tăng/giảm % của từng mã chỉ tiêu so với kỳ liền trước...")
        # [FIX] orderBy theo sort_key số học, KHÔNG orderBy theo quy_danh_gia string
        window_spec = Window.partitionBy("ma_chi_tieu").orderBy("quy_danh_gia_sort_key")

        df_comparison = (
            df_silver
            .withColumn("quy_danh_gia_ky_truoc", lag("quy_danh_gia").over(window_spec))
            .withColumn("muc_dat_numeric_ky_truoc", lag("muc_dat_numeric").over(window_spec))
            .withColumn(
                "tang_truong_phan_tram",
                when(
                    (col("muc_dat_numeric_ky_truoc").isNotNull()) & (col("muc_dat_numeric_ky_truoc") != 0),
                    round(
                        (col("muc_dat_numeric") - col("muc_dat_numeric_ky_truoc"))
                        / col("muc_dat_numeric_ky_truoc") * 100, 2
                    )
                ).otherwise(None)  # kỳ đầu tiên hoặc chỉ tiêu không phải dạng số -> NULL, không phải lỗi
            )
            .withColumn("thoi_gian_dong_goi_gold", current_timestamp())
        )
        df_comparison = with_ten_phong_ban(df_comparison)
        df_comparison = df_comparison.select(*GOLD_COMPARISON_COLUMNS)

        print("\n3. PREVIEW DATA MART SO SÁNH GIỮA CÁC KỲ:")
        df_comparison.filter(col("tang_truong_phan_tram").isNotNull()) \
            .orderBy(col("ma_chi_tieu").asc(), col("quy_danh_gia").asc()) \
            .show(50, truncate=False)

        # ---------------------------------------------------------
        # DATA MART 4: "CHÚ THÍCH / DATA DICTIONARY" CHO MÃ CHỈ TIÊU  [MỚI - Yêu cầu 3]
        # ---------------------------------------------------------
        print("⚙️ Nghiệp vụ 4: Xây bảng chú thích (data dictionary) cho từng mã chỉ tiêu...")

        # struct(sort_key, quy_danh_gia) để MIN/MAX so sánh đúng theo số trước,
        # rồi lấy lại chuỗi quy_danh_gia tương ứng — tránh lỗi so sánh string thuần.
        df_keyed = df_silver.withColumn(
            "ky_struct", struct(col("quy_danh_gia_sort_key"), col("quy_danh_gia"))
        )

        df_dict = (
            df_keyed.groupBy("ma_chi_tieu", "nhom_don_vi")
            .agg(
                first("noi_dung_muc_tieu", ignorenulls=True).alias("noi_dung_muc_tieu"),
                spark_min("ky_struct").alias("_ky_dau_tien_struct"),
                spark_max("ky_struct").alias("_ky_gan_nhat_struct"),
            )
            .withColumn("ky_dau_tien_xuat_hien", col("_ky_dau_tien_struct.quy_danh_gia"))
            .withColumn("ky_gan_nhat_cap_nhat", col("_ky_gan_nhat_struct.quy_danh_gia"))
            .drop("_ky_dau_tien_struct", "_ky_gan_nhat_struct")
        )

        df_dict = with_ten_phong_ban(df_dict)
        df_dict = (
            df_dict
            .withColumn("nguon_bang_chi_tiet", lit(GOLD_DETAIL_TABLE))
            .withColumn("nguon_bang_tong_hop", lit(GOLD_SUMMARY_TABLE))
            .withColumn("nguon_bang_so_sanh_ky", lit(GOLD_COMPARISON_TABLE))
            .withColumn("cot_khoa_join", lit("ma_chi_tieu"))
            .withColumn("thoi_gian_dong_goi_gold", current_timestamp())
            .select(*GOLD_DICT_COLUMNS)
        )

        print("\n4. PREVIEW DATA DICTIONARY (dm_chi_tieu):")
        df_dict.orderBy(col("nhom_don_vi").asc(), col("ma_chi_tieu").asc()).show(50, truncate=False)

        # ---------------------------------------------------------
        # GHI DỮ LIỆU LÊN BRANCH TẠM (chưa ảnh hưởng main)
        # ---------------------------------------------------------
        print(f"🧊 Đang ghi Data Mart Tổng hợp lên branch '{branch_name}'...")
        df_summary.writeTo(GOLD_SUMMARY_TABLE).createOrReplace()

        print(f"🧊 Đang ghi Data Mart Chi tiết lên branch '{branch_name}'...")
        df_detail.writeTo(GOLD_DETAIL_TABLE).createOrReplace()

        print(f"🧊 Đang ghi Data Mart So sánh kỳ lên branch '{branch_name}'...")
        df_comparison.writeTo(GOLD_COMPARISON_TABLE).createOrReplace()

        print(f"🧊 Đang ghi Data Dictionary (dm_chi_tieu) lên branch '{branch_name}'...")
        df_dict.writeTo(GOLD_DICT_TABLE).createOrReplace()

        # Data quality check TRÊN BRANCH trước khi merge (2 bảng gốc)
        check_quality_gold(spark, GOLD_SUMMARY_TABLE, GOLD_DETAIL_TABLE)

        merge_branch_to_main(spark, branch_name)
        use_main(spark)

        print("\n🌟 HOÀN THÀNH TOÀN BỘ PIPELINE: BRONZE -> SILVER -> GOLD THÀNH CÔNG RỰC RỠ!")
        print(f"    Branch '{branch_name}' đã merge vào main và được giữ lại để audit.")

        try:
            om_client = get_client()
            silver_fqn = "lakehouse-trino.lakehouse.silver.kpi_cusc_master"
            gold_summary_fqn = "lakehouse-trino.lakehouse.gold.kpi_tong_hop_don_vi"
            gold_detail_fqn = "lakehouse-trino.lakehouse.gold.kpi_chi_tiet_dashboard"
            gold_comparison_fqn = "lakehouse-trino.lakehouse.gold.kpi_so_sanh_ky"
            gold_dict_fqn = "lakehouse-trino.lakehouse.gold.dm_chi_tieu"

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
                description="Đồng bộ bảng chi tiết phục vụ tra cứu (kèm ten_phong_ban), "
                             "chạy bởi spark_silver_to_gold.py",
            )
            push_lineage_safe(
                om_client, silver_fqn, gold_comparison_fqn,
                sql_query=(
                    "INSERT OVERWRITE gold.kpi_so_sanh_ky "
                    "SELECT ma_chi_tieu, quy_danh_gia, muc_dat_numeric, "
                    "LAG(muc_dat_numeric) OVER (PARTITION BY ma_chi_tieu ORDER BY quy_danh_gia_sort_key) "
                    "FROM silver.kpi_cusc_master"
                ),
                description="So sánh tăng/giảm % của từng mã chỉ tiêu giữa 2 kỳ liên tiếp, "
                             "chạy bởi spark_silver_to_gold.py",
            )
            push_lineage_safe(
                om_client, silver_fqn, gold_dict_fqn,
                sql_query=(
                    "INSERT OVERWRITE gold.dm_chi_tieu "
                    "SELECT ma_chi_tieu, nhom_don_vi, MIN(quy_danh_gia), MAX(quy_danh_gia) "
                    "FROM silver.kpi_cusc_master GROUP BY ma_chi_tieu, nhom_don_vi"
                ),
                description="Bảng chú thích/data dictionary: phòng ban phụ trách, bảng/cột nguồn, "
                             "kỳ đầu tiên & gần nhất của mỗi mã chỉ tiêu, chạy bởi spark_silver_to_gold.py",
            )
        except Exception as e:
            print(f"⚠️  Không đẩy được lineage lên OpenMetadata (bỏ qua, không ảnh hưởng dữ liệu): {e}")

    except DataQualityError as dqe:
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