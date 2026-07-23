from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from db.database import init_db
from api.routes import auth, upload, catalog, pipeline_preview

app = FastAPI(
    title="University Data Lakehouse API",
    description="Hệ thống API quản lý Data Lakehouse, Authentication, MinIO Storage và Pipeline Execution",
    version="1.0.0"
)

# Cấu hình CORS cho phép Frontend React gọi API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Khởi tạo DB & seed user mặc định khi ứng dụng khởi chạy
@app.on_event("startup")
def on_startup():
    init_db()

# Include Routers
app.include_router(auth.router, tags=["Authentication"])
app.include_router(auth.router, prefix="/auth", tags=["Authentication"])
app.include_router(upload.router, prefix="/api", tags=["Upload"])
app.include_router(catalog.router, prefix="/api", tags=["Catalog Versioning"])
app.include_router(pipeline_preview.router, prefix="/pipeline", tags=["Pipeline Explorer"])

@app.get("/", tags=["Health Check"])
async def root():
    return {
        "status": "online",
        "message": "Welcome to University Data Lakehouse API!",
        "version": "1.0.0"
    }