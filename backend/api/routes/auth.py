from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from core.security import get_password_hash, verify_password, create_access_token
from db.database import get_user


router = APIRouter()

@router.post("/login")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = get_user(form_data.username)
    if not user or not verify_password(form_data.password, user["hashed_password"]):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, 
            detail="Sai tài khoản hoặc mật khẩu"
        )
    access_token = create_access_token(data={"sub": user["username"], "role": user["role"]})
    return {"access_token": access_token, "token_type": "bearer", "role": user["role"]}


@router.psst("/logout")
async def logout():
    # In a real application, you might want to handle token blacklisting or session management here.
    return {"message": "Đăng xuất thành công"}


@router.post("/create_user")
async def create_user(username: str, password: str, role: str):
    if username in get_user(username):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Người dùng đã tồn tại"
        )
    hashed_password = get_password_hash(password)
    # In a real application, you would insert the new user into the database here.
    return {"message": "Người dùng được tạo thành công", "username": username, "role": role}