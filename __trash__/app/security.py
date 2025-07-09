# Logic bảo mật (password hashing, JWT)

from datetime import datetime, timedelta, timezone
from jose import JWTError, jwt
from passlib.context import CryptContext
from typing import Optional

from .config import settings

# Khởi tạo context cho việc băm mật khẩu
# bcrypt là thuật toán được khuyến nghị
pwd_context = CryptContext(schemes=['bcrypt'], deprecated='auto')

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Xác thực mật khẩu người dùng nhập vào với mật khẩu đã được băm trong CSDL.

    Args:
        plain_password: Mật khẩu dạng chuỗi thuần.
        hashed_password: Mật khẩu đã được băm.

    Returns:
        True nếu mật khẩu khớp, False nếu không.
    """
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str) -> str:
    """
    Băm mật khẩu dạng chuỗi thuần.

    Args:
        password: Mật khẩu cần băm.

    Returns:
        Chuỗi mật khẩu đã được băm.
    """
    return pwd_context.hash(password)

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """
    Tạo một JSON Web Token (JWT) mới.

    Args:
        data: Dữ liệu cần đưa vào payload của token (ví dụ: username).
        expires_delta: Thời gian sống của token. Nếu không có, sẽ dùng giá trị mặc định.

    Returns:
        Chuỗi access token đã được mã hóa.
    """
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    
    to_encode.update({'exp': expire})

    return jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
