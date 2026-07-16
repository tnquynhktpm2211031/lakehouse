from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends, HTTPException
from api.dependencies import get_current_user
from pathlib import Path
import json
import requests
from datetime import datetime

router = APIRouter()


def _log_file_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "ingestion_logs.jsonl"


def _update_log_entry(file_id: str, updates: dict) -> bool:
    p = _log_file_path()
    if not p.exists():
        return False

    updated = False
    lines = []
    with open(p, "r", encoding="utf-8") as fh:
        for line in fh:
            try:
                obj = json.loads(line)
            except Exception:
                lines.append(line)
                continue
            if obj.get("id") == file_id:
                obj.update(updates)
                updated = True
            lines.append(json.dumps(obj, ensure_ascii=False) + "\n")

    if not updated:
        return False

    with open(p, "w", encoding="utf-8") as fh:
        fh.writelines(lines)

    return True


def trigger_pipeline_for_file(file_id: str) -> dict:
    """Trigger Airflow DAG run for the given file_id and update ingestion log."""
    try:
        from core.config import AIRFLOW_WEBSERVER_URL
    except Exception:
        AIRFLOW_WEBSERVER_URL = None

    if not AIRFLOW_WEBSERVER_URL:
        raise RuntimeError("AIRFLOW_WEBSERVER_URL not configured")

    dag_run_id = f"ingest_{file_id}_{int(datetime.utcnow().timestamp())}"
    airflow_url = f"{AIRFLOW_WEBSERVER_URL}/api/v1/dags/lakehouse_pipeline/dagRuns"
    payload = {"dag_run_id": dag_run_id, "conf": {"file_id": file_id}}

    try:
        resp = requests.post(airflow_url, json=payload, auth=("airflow", "airflow"), timeout=10)
        status_code = resp.status_code
    except Exception as e:
        # update log as failed to queue
        _update_log_entry(file_id, {"pipeline_stage": "Staging", "status": "QUEUE_FAILED", "http_status": 500})
        raise

    if status_code in (200, 201):
        _update_log_entry(file_id, {"pipeline_stage": "Queued", "status": "QUEUED", "http_status": status_code})
        try:
            from api.notifications import notify_event

            notify_event({"event": "FILE_STATUS_UPDATED", "data": {"id": file_id, "pipeline_stage": "Queued", "status": "QUEUED"}})
        except Exception:
            pass
        return {"code": status_code, "message": "Queued"}
    else:
        _update_log_entry(file_id, {"pipeline_stage": "Staging", "status": "QUEUE_FAILED", "http_status": status_code})
        return {"code": status_code, "message": "Failed to queue", "detail": resp.text}


@router.get("/logs")
def list_logs(current_user: dict = Depends(get_current_user)):
    """List ingestion logs (latest first)."""
    p = _log_file_path()
    if not p.exists():
        return []
    items = []
    with open(p, "r", encoding="utf-8") as fh:
        for line in fh:
            try:
                items.append(json.loads(line))
            except Exception:
                continue
    # return newest first
    items.sort(key=lambda x: x.get("receive_time") or "", reverse=True)
    return items


@router.post("/{file_id}/status")
def update_status(file_id: str, payload: dict, current_user: dict = Depends(get_current_user)):
    """Update pipeline_stage/status for a given file. Intended to be called by Airflow after job steps."""
    p = _log_file_path()
    if not p.exists():
        raise HTTPException(status_code=404, detail="No ingestion logs found")

    updated = False
    lines = []
    with open(p, "r", encoding="utf-8") as fh:
        for line in fh:
            try:
                obj = json.loads(line)
            except Exception:
                lines.append(line)
                continue
            if obj.get("id") == file_id:
                # update fields present in payload
                for k in ("pipeline_stage", "status", "http_status"):
                    if k in payload:
                        obj[k] = payload[k]
                updated = True
            lines.append(json.dumps(obj, ensure_ascii=False) + "\n")

    if not updated:
        raise HTTPException(status_code=404, detail="file_id not found")

    with open(p, "w", encoding="utf-8") as fh:
        fh.writelines(lines)

    # optional: notify websocket clients
    try:
        from api.notifications import notify_event

        notify_event({"event": "FILE_STATUS_UPDATED", "data": {"id": file_id, **payload}})
    except Exception:
        pass

    return {"code": 200, "message": "Updated"}


@router.post("/{file_id}/trigger")
def trigger_pipeline(file_id: str, current_user: dict = Depends(get_current_user)):
    """API endpoint to trigger the processing pipeline for a given file_id.
    This will call Airflow to create a DAG run and update the ingestion log."""
    try:
        result = trigger_pipeline_for_file(file_id)
    except RuntimeError as re:
        raise HTTPException(status_code=500, detail=str(re))
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to call Airflow: {e}")

    return result


@router.websocket("/ws")
async def websocket_ws(websocket: WebSocket):
    # allow anonymous websocket connections for notifications
    try:
        from api.notifications import websocket_endpoint

        await websocket_endpoint(websocket)
    except Exception:
        try:
            await websocket.close()
        except Exception:
            pass
