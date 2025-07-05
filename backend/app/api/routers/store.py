from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from typing import List

from ... import services, schemas
from ...core.database import get_db

router = APIRouter()

@router.get('/', response_model = List[schemas.StoreSchema])
def read_stores(db: Session = Depends(get_db)):
    return services.get_all_stores(db)
