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
import boto3
import pandas as pd
import pdfplumber
import docx

from env_config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET_NAME

BUCKET_NAME   = MINIO_BUCKET_NAME
SOURCE_PREFIX = "bronze/unstructured_data/"
OUTPUT_KEY    = "bronze/structured_data/data_extracted.parquet"

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
                    if not r or len(r) < 6: continue
                    ma = extract_ma_chi_tieu(r[0])
                    if ma:
                        dinh_ky = str(r[2]).strip().replace("\n", " ") if r[2] else "N/A"
                        muc_dang_ky = str(r[3]).strip().replace("\n", " ") if r[3] else "N/A"
                        muc_dat = str(r[4]).strip().replace("\n", " ") if r[4] else "N/A"
                        ket_qua = clean_status_text(r[5])
                        rows.append((ma, dinh_ky, muc_dang_ky, muc_dat, ket_qua))
    return rows

def parse_docx(file_bytes: bytes) -> list:
    doc = docx.Document(io.BytesIO(file_bytes))
    rows = []
    for table in doc.tables:
        for row in table.rows:
            cells = [cell.text.strip() for cell in row.cells]
            if len(cells) >= 6:
                # Trường hợp file Word viết tách cột Mã đơn vị và Mã mục tiêu (VD: QTCL | MT001)
                ma_combined = f"{cells[0]}-{cells[1]}" if "-" not in cells[0] and "MT" in cells[1] else cells[0]
                ma = extract_ma_chi_tieu(ma_combined)
                if ma:
                    dinh_ky = cells[3] if cells[3] else "N/A"
                    muc_dang_ky = cells[5] if len(cells) > 5 else "N/A"
                    muc_dat = cells[6] if len(cells) > 6 else "N/A"
                    ket_qua = clean_status_text(cells[7] if len(cells) > 7 else cells[-1])
                    rows.append((ma, dinh_ky, muc_dang_ky, muc_dat, ket_qua))
    return rows

def main():
    s3_client = get_s3_client()
    extracted_data = []
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=SOURCE_PREFIX)
    if "Contents" not in response: sys.exit(0)

    for obj in response["Contents"]:
        file_key = obj["Key"]
        if file_key.endswith('/'): continue
        ext = os.path.splitext(file_key)[1].lower()
        file_bytes = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)["Body"].read()

        raw_rows = parse_pdf(file_bytes) if ext == '.pdf' else (parse_docx(file_bytes) if ext == '.docx' else [])
        
        for ma, dk, m_dk, m_dat, kq in raw_rows:
            nhom = ma.split("-")[0]
            checksum = generate_checksum(f"{file_key}_{ma}_{dk}_{m_dk}_{m_dat}_{kq}")
            extracted_data.append({
                "file_nguon": os.path.basename(file_key),
                "ma_chi_tieu": ma,
                "nhom_don_vi": nhom,
                "quy_danh_gia": "Q1/2026",
                "dinh_ky_thu_thap": dk,
                "muc_dang_ky": m_dk,
                "muc_dat": m_dat,
                "ket_qua_he_thong": kq,
                "checksum_sha256": checksum
            })

    if extracted_data:
        df = pd.DataFrame(extracted_data)
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
        s3_client.put_object(Bucket=BUCKET_NAME, Key=OUTPUT_KEY, Body=parquet_buffer.getvalue())
        print("🌟 HOÀN THÀNH BRONZE OMNI-PARSER RICH SCHEMA!")

if __name__ == "__main__": main()