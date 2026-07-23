from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session
from core.security import decode_access_token
from db.database import get_db, get_user

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

async def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    """Lấy thông tin người dùng hiện tại từ JWT access token."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Không thể xác thực thông tin đăng nhập",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    payload = decode_access_token(token)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token đã hết hạn hoặc không hợp lệ",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    username: str = payload.get("sub")
    if username is None:
        raise credentials_exception
    
    user = get_user(username=username, db=db)
    if user is None:
        raise credentials_exception
    
    # Nếu user là ORM object hoặc dict
    is_active = getattr(user, "is_active", True) if not isinstance(user, dict) else user.get("is_active", True)
    if not is_active:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Tài khoản đã bị khóa")
        
    return user

async def get_current_active_admin(current_user = Depends(get_current_user)):
    """Dependency kiểm tra người dùng có quyền Admin hay không."""
    role = getattr(current_user, "role", None) if not isinstance(current_user, dict) else current_user.get("role")
    if role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Bạn không có quyền thực hiện thao tác này (Yêu cầu quyền Admin)"
        )
    return current_user
