from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from typing import List

from .. import services, models
from ..core.database import get_db

router = APIRouter()

@router.get('/recent', response_model = List[models.ErrLogSchema])
def read_recent_errors(db: Session = Depends(get_db)):
    return services.get_recent_errors(db)
