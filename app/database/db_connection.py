from psycopg2.pool import SimpleConnectionPool
import os
from dotenv import load_dotenv

_pool: SimpleConnectionPool = None

def get_pool() -> SimpleConnectionPool:
    global _pool
    if _pool is None:
        raise RuntimeError("Connection pool has not been initialized.")
    return _pool

async def start_conn():
    load_dotenv()
    global _pool
    try:
        _pool = SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            database=os.getenv('DATABASE_NAME'),
            user=os.getenv('DATABASE_USER'),
            password=os.getenv('DATABASE_PASSWORD'),
            host=os.getenv('DATABASE_HOST', 'localhost'),
            port=os.getenv('DATABASE_PORT')
        )
        if not _pool:
            raise Exception("Failed to initialize connection pool")
        print(f"Database connection pool with {id(_pool)} created successfully.")
    except Exception as e:
        print(f"Error while creating the connection pool: {e}")
        raise

async def shutdown_conn():
    global _pool
    if _pool:
        _pool.closeall()
        print("Database connection pool closed successfully.")
        