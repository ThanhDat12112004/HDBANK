from app.db.base import BaseModel
from typing import Optional, Any
from datetime import datetime

class Subject(BaseModel):
    def __init__(
        self,
        subject_id: str,
        subject_name: str,
        credits: Optional[int] = None,
    ):
        self.subject_id = subject_id
        self.subject_name = subject_name
        self.credits = credits

    @classmethod
    async def create_table(cls, conn: Any) -> None:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS Subjects (
                SubjectID VARCHAR(50) PRIMARY KEY,
                SubjectName VARCHAR(100) NOT NULL,
                Credits INTEGER,
                CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)