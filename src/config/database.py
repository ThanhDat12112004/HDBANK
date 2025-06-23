import asyncpg
import os
from dotenv import load_dotenv
import logging

load_dotenv()
logger = logging.getLogger(__name__)

async def get_db():
    POSTGRES_URI = os.getenv("POSTGRES_URI")
    if not POSTGRES_URI:
        logger.error("POSTGRES_URI is not set in environment variables")
        raise Exception("POSTGRES_URI is missing")
    
    pool = await asyncpg.create_pool(POSTGRES_URI)
    try:
        async with pool.acquire() as conn:
            yield conn
    finally:
        await pool.close()
        logger.info("PostgreSQL connection closed.")