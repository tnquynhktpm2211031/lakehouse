from core.security import get_password_hash

# Giả lập Database (PostgreSQL)
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

def get_user(username: str):
    return fake_users_db.get(username)
