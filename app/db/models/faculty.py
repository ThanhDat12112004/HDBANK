from app.db.base import BaseModel
from typing import Any
from datetime import datetime

class Faculty(BaseModel):
    def __init__(
        self,
        faculty_id: str,
        faculty_name: str,
    ):
        self.faculty_id = faculty_id
        self.faculty_name = faculty_name

    @classmethod
    async def create_table(cls, conn: Any) -> None:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS Faculties (
                FacultyID VARCHAR(50) PRIMARY KEY,
                FacultyName VARCHAR(100) NOT NULL,
                CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)