# Cho phép nhúng Dashboard
FEATURE_FLAGS = {
    "EMBEDDED_SUPERSET": True
}

# Tắt chặn iFrame (X-Frame-Options)
TALISMAN_ENABLED = False

# Cho phép Superset được nhúng trong iframe từ các origin khác
X_FRAME_OPTIONS = "ALLOWALL"

# Thiết lập cookie để không bị chặn khi nhúng (dev local)
SESSION_COOKIE_SAMESITE = None

# Mở CORS để React ở cổng 3000 có thể gọi API vào Superset ở cổng 8088
ENABLE_CORS = True
CORS_OPTIONS = {
    'supports_credentials': True,
    'allowed_origins': ['http://localhost:3000'],
    'always_send': True
}

# Tắt tạm bảo mật CSRF cho môi trường Dev local
WTF_CSRF_ENABLED = False