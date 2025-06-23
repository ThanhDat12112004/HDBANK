import asyncpg
from app.core.config import settings
import logging
from typing import AsyncGenerator

logger = logging.getLogger(__name__)

async def get_db() -> AsyncGenerator:
    pool = await asyncpg.create_pool(settings.POSTGRES_URI)
    logger.info("Connecting to the database...")
    try:
        async with pool.acquire() as conn:
            yield conn
            logger.info("Connected to the database successfully")
    finally:
        await pool.close()
        logger.info("Closed the database connection pool")