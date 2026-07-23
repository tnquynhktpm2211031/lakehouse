from core.security import get_password_hash
import os
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Giả lập Database (PostgreSQL) — giữ để tương thích
fake_users_db = {
    "admin": {
        "username": "admin",
        "hashed_password": get_password_hash("admin123"),
        "role": "admin"
    },
    "canbo_truongA": {
        "username": "canbo_truongA",
        "hashed_password": get_password_hash("user123"),
        "role": "user"
    }
}


# Read DB connection from environment; fallback to provided credentials
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "240203")
PG_HOST = os.getenv("PG_HOST", "127.0.0.1")
PG_PORT = os.getenv("PG_PORT", "5432")
# Default database name: university_db (can be overridden with PG_DATABASE)
PG_DATABASE = os.getenv("PG_DATABASE", "university_db")

# Build SQLAlchemy-compatible URL
_pw = quote_plus(PG_PASSWORD)
DATABASE_URL = f"postgresql://{PG_USER}:{_pw}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"

# Create engine and session factory
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    """Yield a SQLAlchemy session for dependency injection."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_user(username: str):
    """Fallback in-memory user lookup (kept for compatibility).

    To query the real database, inject `get_db` and run queries against
    your users table (schema not assumed here).
    """
    return fake_users_db.get(username)
