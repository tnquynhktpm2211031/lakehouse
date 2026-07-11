# -*- coding: utf-8 -*-
"""
nessie_catalog_utils.py
------------------------------------------------------------
Các hàm tiện ích dùng chung cho việc quản lý phiên bản catalog
qua Nessie (branch / merge / tag) trong các pipeline Spark.

Nessie hoạt động như Git cho dữ liệu:
  - Mỗi lần chạy pipeline -> tạo 1 branch riêng (giống 1 feature branch)
  - Chạy transform + kiểm tra chất lượng dữ liệu TRÊN branch đó
  - Nếu đạt chất lượng -> merge branch vào main
  - Nếu không đạt -> giữ nguyên branch để debug, KHÔNG merge vào main
  - Cuối mỗi kỳ báo cáo -> gắn tag trên main để phục vụ "time travel"

Đặt file này cùng thư mục spark/ để các script khác import được:
    from nessie_catalog_utils import ...
------------------------------------------------------------
"""

from datetime import datetime

CATALOG_NAME = "lakehouse"  # trùng với spark.sql.catalog.lakehouse


def make_branch_name(prefix: str) -> str:
    """Sinh tên branch duy nhất theo thời điểm chạy job.
    Nessie ref name không được chứa dấu ':' nên dùng định dạng YYYYMMDD_HHMMSS."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{ts}"


def create_branch(spark, branch_name: str, from_ref: str = "main"):
    """Tạo branch mới trong Nessie, xuất phát từ 1 ref có sẵn (mặc định main)."""
    print(f"🌿 Đang tạo branch Nessie: '{branch_name}' (từ '{from_ref}')...")
    spark.sql(f"CREATE BRANCH IF NOT EXISTS {branch_name} IN {CATALOG_NAME} FROM {from_ref}")
    print(f"✅ Đã tạo branch '{branch_name}'.")


def use_branch(spark, branch_name: str):
    """Chuyển context làm việc của Spark session sang branch chỉ định.
    Mọi câu lệnh SQL (INSERT/MERGE/OVERWRITE) sau lệnh này sẽ chỉ tác động
    lên branch này, KHÔNG ảnh hưởng tới 'main' cho tới khi merge."""
    spark.sql(f"USE REFERENCE {branch_name} IN {CATALOG_NAME}")
    print(f"➡️  Đang làm việc trên branch: '{branch_name}'")


def use_main(spark):
    """Quay trở lại làm việc trên nhánh main (gọi sau khi merge, hoặc khi rollback lỗi)."""
    spark.sql(f"USE REFERENCE main IN {CATALOG_NAME}")


def merge_branch_to_main(spark, branch_name: str):
    """Merge branch vào main sau khi đã pass data quality check."""
    print(f"🔀 Đang merge branch '{branch_name}' vào 'main'...")
    spark.sql(f"MERGE BRANCH {branch_name} INTO main IN {CATALOG_NAME}")
    print(f"✅ Đã merge '{branch_name}' -> 'main' thành công.")


def drop_branch(spark, branch_name: str):
    """Xoá branch (TUỲ CHỌN, dùng khi muốn dọn dẹp sau khi đã merge xong).
    Mặc định pipeline KHÔNG gọi hàm này để giữ lại lịch sử phục vụ audit/demo."""
    spark.sql(f"DROP BRANCH IF EXISTS {branch_name} IN {CATALOG_NAME}")
    print(f"🗑️  Đã xoá branch '{branch_name}'.")


def create_tag(spark, tag_name: str, from_ref: str = "main"):
    """Gắn tag (mốc phiên bản cố định, không di chuyển) lên 1 ref,
    thường dùng vào cuối mỗi kỳ báo cáo để phục vụ time-travel/audit."""
    print(f"🏷️  Đang tạo tag '{tag_name}' từ '{from_ref}'...")
    spark.sql(f"CREATE TAG IF NOT EXISTS {tag_name} IN {CATALOG_NAME} FROM {from_ref}")
    print(f"✅ Đã tạo tag '{tag_name}'. Time-travel bằng: SELECT * FROM <bảng>@{tag_name}")


class DataQualityError(Exception):
    """Raise khi dữ liệu trên branch không đạt yêu cầu chất lượng tối thiểu."""
    pass


def check_quality_silver(spark, table_name: str):
    """
    Kiểm tra chất lượng dữ liệu tối thiểu cho bảng Silver TRÊN BRANCH hiện tại
    (giả định spark session đã được use_branch() trước khi gọi hàm này):
      1. Bảng phải có ít nhất 1 dòng dữ liệu (không rỗng)
      2. Không có bản ghi thiếu ma_chi_tieu hoặc ket_qua_he_thong (NULL)
      3. Không có checksum_sha256 bị trùng lặp (đảm bảo tính duy nhất bản ghi)
    Raise DataQualityError nếu bất kỳ điều kiện nào không đạt.
    """
    print("🔍 Đang kiểm tra chất lượng dữ liệu Silver trên branch hiện tại...")
    df = spark.table(table_name)

    total_rows = df.count()
    if total_rows == 0:
        raise DataQualityError("Bảng Silver rỗng, không có dữ liệu để merge.")

    null_key_rows = df.filter(
        "ma_chi_tieu IS NULL OR ket_qua_he_thong IS NULL"
    ).count()
    if null_key_rows > 0:
        raise DataQualityError(
            f"Phát hiện {null_key_rows} bản ghi thiếu ma_chi_tieu hoặc ket_qua_he_thong."
        )

    total_checksum = df.select("checksum_sha256").count()
    distinct_checksum = df.select("checksum_sha256").distinct().count()
    if total_checksum != distinct_checksum:
        dup_count = total_checksum - distinct_checksum
        raise DataQualityError(
            f"Phát hiện {dup_count} bản ghi có checksum_sha256 bị trùng lặp."
        )

    print(f"✅ Dữ liệu đạt chất lượng: {total_rows} dòng, không NULL khoá chính, "
          f"không trùng checksum.")
    return True


def check_quality_gold(spark, summary_table: str, detail_table: str):
    """
    Kiểm tra chất lượng tối thiểu cho 2 bảng Gold TRÊN BRANCH hiện tại:
      1. Bảng tổng hợp không rỗng
      2. Tỷ lệ hoàn thành (%) phải nằm trong khoảng hợp lệ [0, 100]
      3. Bảng chi tiết không rỗng
    """
    print("🔍 Đang kiểm tra chất lượng Data Mart Gold trên branch hiện tại...")

    df_summary = spark.table(summary_table)
    summary_count = df_summary.count()
    if summary_count == 0:
        raise DataQualityError("Bảng Gold tổng hợp (kpi_tong_hop_don_vi) rỗng.")

    invalid_ratio_rows = df_summary.filter(
        "ty_le_hoan_thanh_phan_tram < 0 OR ty_le_hoan_thanh_phan_tram > 100"
    ).count()
    if invalid_ratio_rows > 0:
        raise DataQualityError(
            f"Phát hiện {invalid_ratio_rows} dòng có tỷ lệ hoàn thành ngoài khoảng [0, 100]."
        )

    df_detail = spark.table(detail_table)
    detail_count = df_detail.count()
    if detail_count == 0:
        raise DataQualityError("Bảng Gold chi tiết (kpi_chi_tiet_dashboard) rỗng.")

    print(f"✅ Data Mart Gold đạt chất lượng: "
          f"{summary_count} dòng tổng hợp, {detail_count} dòng chi tiết.")
    return True