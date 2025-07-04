from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from datetime import timedelta
from passlib.context import CryptContext

from ...core.config import settings
from ...schemas import schemas
from .security import create_access_token # Sẽ tạo file này ngay sau đây

router = APIRouter()

# --- HARDCODED USER DATA ---
# Thay "your_very_secure_password" bằng mật khẩu bạn muốn
HARDCODED_USERNAME = "root"
# Tạo một password hash an toàn, không bao giờ lưu password dạng plain text
# Chạy python và gõ: from passlib.context import CryptContext; CryptContext(schemes=["bcrypt"]).hash("your_very_secure_password")
# Copy kết quả vào đây
HARDCODED_PASSWORD_HASH = "$2b$12$EixZaYVK13nJ3sJ25DVFp.L2x0c8wCR.aG32ED5a1N3E1L6adgqg." # Hash cho "your_very_secure_password"

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

@router.post("/auth/token", response_model=schemas.Token)
def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    """
    Endpoint để đăng nhập và nhận JWT token.
    """
    is_correct_username = form_data.username == HARDCODED_USERNAME
    is_correct_password = pwd_context.verify(form_data.password, HARDCODED_PASSWORD_HASH)

    if not (is_correct_username and is_correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
        
    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": HARDCODED_USERNAME}, expires_delta=access_token_expires
    )
    
    return {"access_token": access_token, "token_type": "bearer"}
