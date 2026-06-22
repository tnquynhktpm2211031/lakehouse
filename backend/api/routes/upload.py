from fastapi import APIRouter, Depends, File, UploadFile, HTTPException
from api.dependencies import get_current_user
from db.minio_client import minio_client
from core.config import MINIO_BUCKET_NAME

router = APIRouter()

@router.post("/upload")
@router.post("/upload/")
async def upload_file(file: UploadFile = File(...), current_user: dict = Depends(get_current_user)):
    try:

        file_ext = file.filename.split('.')[-1].lower()
        if file_ext in ['csv']:
            folder = "structured_data"
        elif file_ext in ['json']:
            folder = "semi_structured_data"
        else:
            folder = "unstructured_data"

        object_name = f"bronze/{folder}/{file.filename}"

        file.file.seek(0, 2)
        file_size = file.file.tell()
        file.file.seek(0)

        minio_client.put_object(
            bucket_name=MINIO_BUCKET_NAME,
            object_name=object_name,
            data=file.file,
            length=file_size
        )

        return {"message": f"Đã đẩy trực tiếp file {file.filename} vào trạm {object_name} của MinIO!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi đẩy file lên MinIO: {str(e)}")
