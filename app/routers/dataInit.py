from fastapi import APIRouter, Depends, HTTPException, status
from typing import List
from psycopg2 import extensions

from app.database import db_methods

router = APIRouter(
    prefix="/dataInit"
)