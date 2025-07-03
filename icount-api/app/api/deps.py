from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt

from ..core.config import settings
from ..schemas import schemas

# Endpoint 'auth/token' là nơi user lấy token
oauth2_scheme = OAuth2PasswordBearer(tokenUrl='/api/v1/auth/token')

def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail='Could not validate credentials',
        headers={'WWW-Authenticate': 'Bearer'},
    )

    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        username: str = payload.get('sub')
        if username is None or username != 'root': # Đảm bảo username trong token phải là 'root'
            raise credentials_exception
        token_data = schemas.TokenData(username=username)
    except JWTError:
        raise credentials_exception

    # Chỉ trả về username đã được xác thực, không cần truy vấn DB
    return token_data.username
