from fastapi import FastAPI, Depends
from fastapi.responses import PlainTextResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from fastapi.middleware.cors import CORSMiddleware
from psycopg2.extensions import connection

from app.database.db_connection import start_conn, shutdown_conn
from app.dependencies import get_db_connection
from app.routers import semanticSearch

app = FastAPI()

app.add_event_handler("startup", start_conn)
app.add_event_handler("shutdown", shutdown_conn)

app.include_router(semanticSearch.router)

origins = [
    # Define Allowed Origin Strings (String where Frontend runs in browser)
    "http://localhost",
    "http://localhost:3000",
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def load_cache_on_startup():
    column_name = "article_type"
    db = next(get_db_connection())
    from app.database.db_methods import get_distinct_column_values
    distinct_values = get_distinct_column_values(db, column_name)
    from app.cache import cache
    cache[column_name] = distinct_values
    print(f"Cache loaded with distinct values for {column_name}")

@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request, exc):
    print(f"{repr(exc)}")
    return PlainTextResponse(str(exc.detail), status_code=exc.status_code)


@app.get("/")
def root():
    return {"message": "Hello World! (From Fast-API)"}