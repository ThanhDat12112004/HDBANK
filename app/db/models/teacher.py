from app.db.base import BaseModel
from typing import Optional, Any
from datetime import datetime

class Teacher(BaseModel):
    def __init__(
        self,
        teacher_id: str,
        full_name: str,
        email: str,
        phone: Optional[str] = None,
    ):
        self.teacher_id = teacher_id
        self.full_name = full_name
        self.email = email
        self.phone = phone

    @classmethod
    async def create_table(cls, conn: Any) -> None:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS Teachers (
                TeacherID VARCHAR(50) PRIMARY KEY,
                FullName VARCHAR(100) NOT NULL,
                Email VARCHAR(100) NOT NULL,
                Phone VARCHAR(20),
                CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)