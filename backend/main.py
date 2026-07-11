from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.routes import auth, upload

app = FastAPI(title="Lakehouse API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include Routers
app.include_router(auth.router, tags=["Authentication"])
app.include_router(upload.router, prefix="/api", tags=["Upload"])

# Catalog Router
from api.routes import catalog
app.include_router(catalog.router, prefix="/api")

@app.get("/")
async def root():
    return {"message": "Welcome to Lakehouse API!"}