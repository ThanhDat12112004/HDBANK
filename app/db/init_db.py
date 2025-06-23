import asyncio
from typing import Any
from app.core.config import settings
from app.db.models.faculty import Faculty
from app.db.models.teacher import Teacher
from app.db.models.class_ import Class
from app.db.models.student import Student
from app.db.models.subject import Subject
from app.db.models.grade import Grade
from app.db.models.account import Account
import asyncpg
import logging

logger = logging.getLogger(__name__)

async def init_db() -> None:
    """Initialize database tables"""
    try:
        print(settings.POSTGRES_URI)
        # Connect to the database
        conn = await asyncpg.connect(settings.POSTGRES_URI)
        
        # Create tables in order (respecting foreign key constraints)
        models = [Faculty, Teacher, Class, Account, Student, Subject, Grade]
        
        for model in models:
            await model.create_table(conn)
            logger.info(f"Created table for {model.__name__}")
        
        await conn.close()
        logger.info("Database initialization completed successfully")
        
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(init_db())