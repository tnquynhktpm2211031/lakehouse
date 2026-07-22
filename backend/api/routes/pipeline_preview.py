# -*- coding: utf-8 -*-
"""
api/routes/pipeline_preview.py
------------------------------------------------------------
Router FastAPI: cung cấp dữ liệu xem trước (preview) cho từng tầng
Bronze / Silver / Gold, phục vụ UI "Pipeline Data Explorer" ở frontend.

Đã sửa so với bản gốc: import get_current_user từ api.dependencies
(khớp với cách api/routes/catalog.py đang import: from api.dependencies import get_current_user).
------------------------------------------------------------
"""

import io
import os
import boto3
import pandas as pd
from fastapi import APIRouter, HTTPException, Depends, Query
from trino.dbapi import connect as trino_connect

# [SỬA] import đúng vị trí thật: api/dependencies.py (xác nhận qua api/routes/catalog.py)
from api.dependencies import get_current_user

router = APIRouter()

# --- Cấu hình (đọc từ biến môi trường, khớp docker-compose hiện có) ---
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET_NAME", "university-lakehouse")

TRINO_HOST     = os.getenv("TRINO_HOST", "trino")
TRINO_PORT     = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER     = os.getenv("TRINO_USER", "admin")
TRINO_CATALOG  = "lakehouse"

# Whitelist bảng Gold hợp lệ (tránh SQL injection qua query param 'table')
GOLD_TABLES = {
    "kpi_tong_hop_don_vi":     "Tổng hợp KPI theo đơn vị",
    "kpi_chi_tiet_dashboard":  "Chi tiết đầy đủ KPI",
    "kpi_so_sanh_ky":          "So sánh tăng/giảm giữa các kỳ",
    "dm_chi_tieu":             "Chú thích / Data Dictionary mã chỉ tiêu",
}


def _get_s3_client():
    return boto3.client(
        "s3", endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY, aws_secret_access_key=MINIO_SECRET_KEY,
    )


def _run_trino_query(sql: str):
    conn = trino_connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog=TRINO_CATALOG)
    cur = conn.cursor()
    cur.execute(sql)
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    return columns, rows


@router.get("/status")
def get_pipeline_status(current_user=Depends(get_current_user)):
    """Trạng thái từng tầng (có dữ liệu hay chưa), để frontend tô xanh/xám
    từng node giống Airflow Graph View."""
    status = {"bronze": False, "silver": False, "gold": False}

    try:
        s3 = _get_s3_client()
        resp = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix="bronze/data_extracted_", MaxKeys=1)
        status["bronze"] = "Contents" in resp and len(resp["Contents"]) > 0
    except Exception:
        status["bronze"] = False

    try:
        _, rows = _run_trino_query("SELECT COUNT(*) FROM silver.kpi_cusc_master")
        status["silver"] = rows[0][0] > 0
    except Exception:
        status["silver"] = False

    try:
        _, rows = _run_trino_query("SELECT COUNT(*) FROM gold.kpi_chi_tiet_dashboard")
        status["gold"] = rows[0][0] > 0
    except Exception:
        status["gold"] = False

    return status


@router.get("/bronze/preview")
def preview_bronze(limit: int = Query(50, ge=1, le=500), current_user=Depends(get_current_user)):
    """Đọc file Parquet Bronze MỚI NHẤT trên MinIO (không qua Trino được vì
    Bronze chỉ là file thô, chưa phải bảng Iceberg)."""
    s3 = _get_s3_client()
    resp_bronze = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix="bronze/data_extracted_")
    resp_archive = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix="bronze_archive/data_extracted_")
    
    contents = []
    if "Contents" in resp_bronze and resp_bronze["Contents"]:
        contents.extend(resp_bronze["Contents"])
    if "Contents" in resp_archive and resp_archive["Contents"]:
        contents.extend(resp_archive["Contents"])
        
    if not contents:
        raise HTTPException(status_code=404, detail="Chưa có file Parquet nào ở tầng Bronze hoặc Archive.")

    latest = max(contents, key=lambda o: o["LastModified"])
    file_bytes = s3.get_object(Bucket=MINIO_BUCKET, Key=latest["Key"])["Body"].read()
    df = pd.read_parquet(io.BytesIO(file_bytes))
    return {
        "layer": "bronze",
        "source_file": latest["Key"],
        "last_modified": latest["LastModified"].isoformat(),
        "total_rows": len(df),
        "columns": df.columns.tolist(),
        "rows": df.head(limit).fillna("").to_dict(orient="records"),
    }


@router.get("/silver/preview")
def preview_silver(limit: int = Query(50, ge=1, le=500), current_user=Depends(get_current_user)):
    """Query trực tiếp bảng Iceberg Silver qua Trino."""
    try:
        columns, rows = _run_trino_query(
            f"SELECT * FROM silver.kpi_cusc_master ORDER BY thoi_gian_ingest_silver DESC LIMIT {limit}"
        )
        _, count_rows = _run_trino_query("SELECT COUNT(*) FROM silver.kpi_cusc_master")
        total_rows = count_rows[0][0]
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Không truy vấn được bảng Silver qua Trino: {e}")

    return {
        "layer": "silver",
        "source_table": "lakehouse.silver.kpi_cusc_master",
        "total_rows": total_rows,
        "columns": columns,
        "rows": [dict(zip(columns, r)) for r in rows],
    }


@router.get("/gold/preview")
def preview_gold(
    table: str = Query(..., description=f"Một trong: {', '.join(GOLD_TABLES.keys())}"),
    limit: int = Query(50, ge=1, le=500),
    current_user=Depends(get_current_user),
):
    """Query 1 trong 4 bảng Gold qua Trino."""
    if table not in GOLD_TABLES:
        raise HTTPException(
            status_code=400,
            detail=f"Bảng '{table}' không hợp lệ. Chọn 1 trong: {list(GOLD_TABLES.keys())}",
        )

    try:
        columns, rows = _run_trino_query(
            f"SELECT * FROM gold.{table} ORDER BY thoi_gian_dong_goi_gold DESC LIMIT {limit}"
        )
        _, count_rows = _run_trino_query(f"SELECT COUNT(*) FROM gold.{table}")
        total_rows = count_rows[0][0]
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Không truy vấn được bảng gold.{table} qua Trino: {e}")

    return {
        "layer": "gold",
        "source_table": f"lakehouse.gold.{table}",
        "table_label": GOLD_TABLES[table],
        "total_rows": total_rows,
        "columns": columns,
        "rows": [dict(zip(columns, r)) for r in rows],
    }


@router.get("/gold/tables")
def list_gold_tables(current_user=Depends(get_current_user)):
    """Danh sách 4 bảng Gold hiện có, để frontend hiện dropdown khi click node 'Gold'."""
    return [{"table": k, "label": v} for k, v in GOLD_TABLES.items()]