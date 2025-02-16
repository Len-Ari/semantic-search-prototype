from fastapi import APIRouter, Depends, HTTPException
from psycopg2.extensions import connection

from app.database import db_methods
from app.dependencies import get_db_connection
from app.schemas import SearchRequest

router = APIRouter(
    prefix="/semanticSearch"
)

@router.get("/distinct/{column_name}")
def get_distinct_values(column_name: str, db: connection = Depends(get_db_connection)):
    from app.cache import cache
    if column_name in cache:
        return {"cached": True, "values": cache[column_name]}
    print(cache)
    try:
        distinct_values = db_methods.get_distinct_column_values(db, column_name)
        cache[column_name] = distinct_values
    except:
        raise HTTPException(status_code=400, detail=f"Column '{column_name}' does not exist!")
    return {"cached": False, "values": distinct_values}


@router.post("/")
async def receive_data(data: SearchRequest, db: connection = Depends(get_db_connection)):
    # Useless now since I allow empty queries rn...
    print(data.dict())
    if data.query == "" and not (data.article_ids or data.fts_aff_aut_jtl):
        raise HTTPException(status_code=400, detail="'query' must be provided if no article-id or fts is provided.")
    if not data.score_embedding_type in ["both", "title", "abstract"]:
        raise HTTPException(status_code=400, detail="Invalid value for score_embedding_type: Choose one of [\"both\", \"title\", \"abstract\"].")
    if not 0 < data.similarity_threshhold < 1:
        raise HTTPException(status_code=400, detail="Invalid value for similarity_threshhold: Must be between 0 and 1!")
    results = db_methods.query_db(db, data.dict())
    return {
        "message": "Data received successfully",
        "received_data": data,
        "results": results
    }