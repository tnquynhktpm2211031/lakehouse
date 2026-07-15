# -*- coding: utf-8 -*-
"""
spark_ingest_bronze.py (Bản mở rộng Schema đầy đủ)
------------------------------------------------------------
TẦNG BRONZE - OMNI-PARSER RICH SCHEMA
Tự động trích xuất: Mã, Định kỳ, Mức đăng ký, Mức đạt, Kết quả quý.
Ghi ra: bronze/structured_data/data_extracted.parquet
------------------------------------------------------------
"""

import io
import os
import sys
import hashlib
import re
import zipfile
import boto3
from datetime import datetime
import pandas as pd
import pdfplumber
import docx

from env_config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET_NAME

BUCKET_NAME   = MINIO_BUCKET_NAME
SOURCE_PREFIX = "staging/"
ARCHIVE_PREFIX = "archive/"

KETQUA_KHONG_DAT = "KHÔNG ĐẠT"
KETQUA_DAT = "ĐẠT"
KETQUA_CHUA_DEN_KY = "CHƯA ĐẾN KỲ ĐÁNH GIÁ"

def get_s3_client():
    return boto3.client("s3", endpoint_url=MINIO_ENDPOINT, aws_access_key_id=MINIO_ACCESS_KEY, aws_secret_access_key=MINIO_SECRET_KEY)

def generate_checksum(text: str) -> str:
    return hashlib.sha256(text.encode('utf-8')).hexdigest()

def clean_status_text(text: str) -> str:
    text = str(text).upper().strip()
    if re.search(r'(KHÔNG ĐẠT|FAILED)', text): return KETQUA_KHONG_DAT
    if re.search(r'(CHƯA ĐẾN KỲ|NOT DUE)', text): return KETQUA_CHUA_DEN_KY
    if re.search(r'(ĐẠT|PASSED|SUCCESS)', text): return KETQUA_DAT
    return text.replace("\n", " ").strip()

def extract_ma_chi_tieu(text: str):
    text_clean = str(text).replace(" ", "").replace("\n", "").upper()
    match = re.search(r'([A-Z]{2,5}-?MT\d{2,3})', text_clean)
    if match:
        ma_raw = match.group(1)
        if "-" not in ma_raw: ma_raw = ma_raw.replace("MT", "-MT")
        return ma_raw
    return None

def parse_pdf(file_bytes: bytes) -> list:
    rows = []
    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if table:
                for r in table:
                    if not r or len(r) < 8: continue
                    ma = extract_ma_chi_tieu(r[0])
                    if ma:
                        noi_dung = str(r[1]).strip().replace("\n", " ") if r[1] else "N/A"
                        dinh_ky = str(r[2]).strip().replace("\n", " ") if r[2] else "N/A"
                        muc_dang_ky = str(r[3]).strip().replace("\n", " ") if r[3] else "N/A"
                        muc_dat = str(r[4]).strip().replace("\n", " ") if r[4] else "N/A"
                        ket_qua = clean_status_text(r[5])
                        nguyen_nhan = str(r[6]).strip().replace("\n", " ") if r[6] else ""
                        hanh_dong = str(r[7]).strip().replace("\n", " ") if r[7] else ""
                        rows.append((ma, noi_dung, dinh_ky, muc_dang_ky, muc_dat, ket_qua, nguyen_nhan, hanh_dong))
    return rows

def parse_docx(file_bytes: bytes) -> list:
    try:
        doc = docx.Document(io.BytesIO(file_bytes))
    except (zipfile.BadZipFile, KeyError, ValueError) as exc:
        raise ValueError("Invalid or corrupt DOCX file") from exc

    rows = []
    for table in doc.tables:
        for row in table.rows:
            cells = [cell.text.strip() for cell in row.cells]
            if len(cells) < 8: continue
            ma = extract_ma_chi_tieu(cells[0])
            if ma:
                noi_dung = cells[1].replace("\n", " ") if cells[1] else "N/A"
                dinh_ky = cells[2].replace("\n", " ") if cells[2] else "N/A"
                muc_dang_ky = cells[3].replace("\n", " ") if cells[3] else "N/A"
                muc_dat = cells[4].replace("\n", " ") if cells[4] else "N/A"
                ket_qua = clean_status_text(cells[5])
                nguyen_nhan = cells[6].replace("\n", " ") if cells[6] else ""
                hanh_dong = cells[7].replace("\n", " ") if cells[7] else ""
                rows.append((ma, noi_dung, dinh_ky, muc_dang_ky, muc_dat, ket_qua, nguyen_nhan, hanh_dong))
    return rows

def main():
    sys.stdout.reconfigure(encoding='utf-8')
    s3_client = get_s3_client()
    extracted_data = []
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=SOURCE_PREFIX)
    if "Contents" not in response: sys.exit(0)

    processed_keys = []
    for obj in response["Contents"]:
        file_key = obj["Key"]
       
        if file_key.endswith('/'): continue
        ext = os.path.splitext(file_key)[1].lower()
        file_bytes = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)["Body"].read()

        raw_rows = []
        if ext == '.pdf':
            raw_rows = parse_pdf(file_bytes)
        elif ext == '.docx':
            try:
                raw_rows = parse_docx(file_bytes)
            except ValueError as exc:
                print(f"WARNING: Bỏ qua file không hợp lệ {file_key}: {exc}")
        else:
            print(f"SKIP: Định dạng không hỗ trợ cho file {file_key}")
        
        for ma, noi_dung, dk, m_dk, m_dat, kq, nguyen_nhan, hanh_dong in raw_rows:
            nhom = ma.split("-")[0]
            checksum = generate_checksum(f"{file_key}_{ma}_{dk}_{m_dk}_{m_dat}_{kq}")
            extracted_data.append({
                "file_nguon": os.path.basename(file_key),
                "ma_chi_tieu": ma,
                "nhom_don_vi": nhom,
                "quy_danh_gia": "Q1/2026",
                "noi_dung_muc_tieu": noi_dung,
                "dinh_ky_thu_thap": dk,
                "muc_dang_ky": m_dk,
                "muc_dat": m_dat,
                "ket_qua_he_thong": kq,
                "nguyen_nhan": nguyen_nhan,
                "hanh_dong_khac_phuc": hanh_dong,
                "checksum_sha256": checksum
            })
        
        processed_keys.append(file_key)

    if extracted_data:
        # 1. Tạo tên file động theo thời gian
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_key = f"bronze/data_extracted_{timestamp}.parquet"

        # 2. Ghi ra Parquet
        df = pd.DataFrame(extracted_data)
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
        s3_client.put_object(Bucket=BUCKET_NAME, Key=output_key, Body=parquet_buffer.getvalue())
        print(f"Đã tạo Parquet: {output_key}")
    else:
        print("Không có dữ liệu hợp lệ nào được trích xuất để ghi Parquet.")

    if processed_keys:
        # 3. Archive các file đã xử lý (kể cả file không parse được) để tránh kẹt lại staging
        for key in processed_keys:
            archive_key = key.replace(SOURCE_PREFIX, ARCHIVE_PREFIX, 1)
            s3_client.copy_object(
                Bucket=BUCKET_NAME,
                CopySource={'Bucket': BUCKET_NAME, 'Key': key},
                Key=archive_key
            )
            s3_client.delete_object(Bucket=BUCKET_NAME, Key=key)

        print(f"HOÀN THÀNH INGEST! Đã dọn dẹp {len(processed_keys)} files khỏi staging.")
    else:
        print("Không có file nào mới để xử lý.")

if __name__ == "__main__": main()