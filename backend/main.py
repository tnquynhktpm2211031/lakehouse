import os
from fastapi import FastAPI, Depends, HTTPException, status, File, UploadFile
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from passlib.context import CryptContext
from jose import JWTError, jwt
from datetime import datetime, timedelta
from minio import Minio # Import thư viện MinIO

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- CẤU HÌNH BẢO MẬT ---
SECRET_KEY = "nhuquynh_data_lakehouse_secret_key"
ALGORITHM = "HS256"
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

# Giả lập Database (PostgreSQL)
fake_users_db = {
    "admin": {
        "username": "admin",
        "hashed_password": pwd_context.hash("admin123"),
        "role": "admin"
    },
    "canbo_truongA": {
        "username": "canbo_truongA",
        "hashed_password": pwd_context.hash("user123"),
        "role": "user"
    }
}

# --- KẾT NỐI MINIO LÕI (DATA LAKEHOUSE) ---
minio_client = Minio(
    "127.0.0.1:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False # Đang dùng localhost HTTP
)
BUCKET_NAME = "university-lakehouse"

# Đảm bảo Bucket luôn sẵn sàng
if not minio_client.bucket_exists(BUCKET_NAME):
    minio_client.make_bucket(BUCKET_NAME)

# --- HÀM HỖ TRỢ JWT ---
def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(hours=2)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

async def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        role: str = payload.get("role")
        if username is None:
            raise HTTPException(status_code=401, detail="Token không hợp lệ")
        return {"username": username, "role": role}
    except JWTError:
        raise HTTPException(status_code=401, detail="Token đã hết hạn hoặc sai")

# --- API 1: ĐĂNG NHẬP ---
@app.post("/login")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = fake_users_db.get(form_data.username)
    if not user or not pwd_context.verify(form_data.password, user["hashed_password"]):
        raise HTTPException(status_code=400, detail="Sai tài khoản hoặc mật khẩu")
    access_token = create_access_token(data={"sub": user["username"], "role": user["role"]})
    return {"access_token": access_token, "token_type": "bearer", "role": user["role"]}

# --- API 2: UPLOAD FILE ĐẨY THẲNG VÀO BRONZE ---
@app.post("/api/upload")
@app.post("/api/upload/")
async def upload_file(file: UploadFile = File(...), current_user: dict = Depends(get_current_user)):
    try:
        # Phân loại cấu trúc thư mục MinIO thông minh theo chuẩn Lakehouse
        file_ext = file.filename.split('.')[-1].lower()
        if file_ext in ['csv']:
            folder = "structured_data"
        elif file_ext in ['json']:
            folder = "semi_structured_data"
        else:
            folder = "unstructured_data"
            
        # Đường dẫn vật lý trên MinIO
        object_name = f"bronze/{folder}/{file.filename}"
        
        # Tính toán dung lượng file để stream trực tiếp lên MinIO
        file.file.seek(0, 2)
        file_size = file.file.tell()
        file.file.seek(0)
        
        # Đẩy thẳng vào hồ dữ liệu (Lakehouse)
        minio_client.put_object(
            bucket_name=BUCKET_NAME,
            object_name=object_name,
            data=file.file,
            length=file_size
        )
            
        return {"message": f"Đã đẩy trực tiếp file {file.filename} vào trạm {object_name} của MinIO!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi đẩy file lên MinIO: {str(e)}")