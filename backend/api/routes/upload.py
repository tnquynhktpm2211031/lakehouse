import logging
import requests
import uuid
import json
from datetime import datetime
from pathlib import Path
from fastapi import APIRouter, Depends, File, UploadFile, HTTPException, Response
from api.dependencies import get_current_user
from db.minio_client import minio_client
from core.config import MINIO_BUCKET_NAME
from api.notifications import notify_event
from api.routes.ingest import trigger_pipeline_for_file

router = APIRouter()
@router.get("/")
def read_root():
    return {"message": "Welcome to the Lakehouse API!"}

@router.post("/upload", status_code=201)
@router.post("/upload/", status_code=201)
async def upload_file(file: UploadFile = File(...), current_user: dict = Depends(get_current_user)):
    try:
       
        # Mọi file người dùng nạp vào đều phải qua tầng Staging.
        object_name = f"staging/{file.filename}"

        file.file.seek(0, 2)
        file_size = file.file.tell()
        file.file.seek(0)

   
        minio_client.put_object(
            bucket_name=MINIO_BUCKET_NAME,
            object_name=object_name,
            data=file.file,
            length=file_size
        )

        # Record ingestion metadata (simple JSONL store)
        data_dir = Path(__file__).resolve().parents[2] / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        log_file = data_dir / "ingestion_logs.jsonl"

        file_id = str(uuid.uuid4())
        meta = {
            "id": file_id,
            "file_name": file.filename,
            "sender": current_user.get("username") if current_user else None,
            "source_system": None,
            "receive_time": datetime.utcnow().isoformat() + "Z",
            "bucket": MINIO_BUCKET_NAME,
            "object_key": object_name,
            "pipeline_stage": "Staging",
            "status": "RECEIVED",
            "http_status": 201,
        }

        with open(log_file, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(meta, ensure_ascii=False) + "\n")

        # broadcast notification to any connected websocket clients
        try:
            notify_event({"event": "NEW_FILE_RECEIVED", "data": meta})
        except Exception:
            logging.exception("Failed to notify websocket clients")
        # Trigger Airflow Pipeline via ingest helper (will update ingestion log)
        try:
            trigger_pipeline_for_file(file_id)
        except Exception:
            logging.exception("Failed to trigger pipeline for file_id %s", file_id)

        return {"code":201, "message": "File received successfully", "fileId": file_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi đẩy file  {str(e)}")