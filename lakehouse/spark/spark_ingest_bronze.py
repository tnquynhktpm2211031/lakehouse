"""
spark_ingest_bronze.py
------------------------------------------------------------
Bóc tách bảng dữ liệu từ các file PDF (báo cáo đánh giá KPI CUSC)
nằm trong bronze/unstructured_data/ trên MinIO, làm sạch, rồi ghi
kết quả dưới dạng Parquet vào bronze/structured_pdf_data/.

Lưu ý: File này KHÔNG dùng Spark (chỉ dùng pandas + boto3 + pdfplumber)
để tránh các vấn đề tương thích Spark Socket trên Windows khi xử lý PDF.
Tầng Silver (spark_bronze_to_silver.py) mới thực sự dùng Spark để đọc
file Parquet này lên và ghi vào Iceberg.
------------------------------------------------------------
"""

import io
import sys
import boto3
# pyrefly: ignore [missing-import]
import pdfplumber
import pandas as pd

# ============================================================
# CẤU HÌNH
# ============================================================
MINIO_ENDPOINT = "http://127.0.0.1:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

BUCKET_NAME = "university-lakehouse"
SOURCE_PREFIX = "bronze/unstructured_data/"
OUTPUT_KEY = "bronze/structured_pdf_data/data_extracted.parquet"

# Nhãn kết quả chuẩn hoá — dùng chung với tầng Silver/Gold để tránh lệch chuỗi
KETQUA_KHONG_DAT = "KHÔNG ĐẠT"
KETQUA_DAT = "ĐẠT"
KETQUA_CHUA_DEN_KY = "CHƯA ĐẾN KỲ ĐÁNH GIÁ"


def get_s3_client():
    print("⏳ Đang kết nối MinIO qua boto3...")
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=boto3.session.Config(signature_version="s3v4"),
    )


def chuan_hoa_ket_qua(ket_qua_raw: str) -> str:
    """Chuẩn hoá chuỗi kết quả hệ thống về 1 trong 3 nhãn cố định."""
    text = ket_qua_raw.strip()
    if "KHÔNG ĐẠT" in text:
        return KETQUA_KHONG_DAT
    if "ĐẠT" in text:
        return KETQUA_DAT
    if "Chưa đến kỳ" in text or "CHƯA ĐẾN KỲ" in text.upper():
        return KETQUA_CHUA_DEN_KY
    return text.replace("\n", " ").strip()


def bóc_tách_pdf(s3_client, file_key: str):
    """Đọc 1 file PDF từ MinIO, trả về list các dòng dữ liệu đã làm sạch."""
    print(f"📄 Đang xử lý bóc tách file: {file_key}")
    file_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
    pdf_bytes = file_obj["Body"].read()

    rows = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if not table:
                continue

            for r in table:
                if not r or len(r) < 6:
                    continue

                ma_raw = str(r[0]).strip()
                if not ma_raw or ma_raw == "None" or "MÃ" in ma_raw.upper():
                    continue

                ma_chi_tieu = ma_raw.replace("\n", "").replace(" ", "")
                nhom_don_vi = ma_chi_tieu.split("-")[0] if "-" in ma_chi_tieu else "KHÁC"

                ket_qua_raw = str(r[5]).strip()
                ket_qua_he_thong = chuan_hoa_ket_qua(ket_qua_raw)

                rows.append((ma_chi_tieu, nhom_don_vi, "Q1/2026", ket_qua_he_thong))

    if not rows:
        print(f"   ⚠️ Không tìm thấy bảng dữ liệu hợp lệ trong: {file_key}")
    else:
        print(f"   ✅ Trích xuất được {len(rows)} dòng từ: {file_key}")

    return rows


def main():
    s3_client = get_s3_client()
    extracted_data = []

    try:
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=SOURCE_PREFIX)

        if "Contents" not in response:
            print(f"⚠️ Không tìm thấy file nào trong: {SOURCE_PREFIX}")
            sys.exit(0)

        pdf_keys = [obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(".pdf")]

        if not pdf_keys:
            print(f"⚠️ Không tìm thấy file .pdf nào trong: {SOURCE_PREFIX}")
            sys.exit(0)

        for file_key in pdf_keys:
            extracted_data.extend(bóc_tách_pdf(s3_client, file_key))

        if not extracted_data:
            print("⚠️ Không tìm thấy bảng dữ liệu nào hợp lệ trong toàn bộ file PDF.")
            sys.exit(0)

        df = pd.DataFrame(
            extracted_data,
            columns=["ma_chi_tieu", "nhom_don_vi", "quy_danh_gia", "ket_qua_he_thong"],
        )

        print("\n✅ Đã trích xuất thành công bảng dữ liệu từ PDF! Xem trước dữ liệu:")
        print(df.to_string(index=False))

        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
        parquet_buffer.seek(0)

        print(f"\n⏳ Đang tải file Parquet sạch lên MinIO tại: {OUTPUT_KEY}")
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=OUTPUT_KEY,
            Body=parquet_buffer.getvalue(),
        )
        print("🌟 HOÀN THÀNH INGESTION TẦNG BRONZE (NATIVE PYTHON)!")

    except Exception as e:
        print(f"❌ Lỗi hệ thống: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()