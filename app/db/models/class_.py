from app.db.base import BaseModel
from typing import Optional, Any
from datetime import datetime

class Class(BaseModel):
    def __init__(
        self,
        class_id: str,
        class_name: str,
        teacher_id: Optional[str] = None,
        faculty_id: Optional[str] = None,
    ):
        self.class_id = class_id
        self.class_name = class_name
        self.teacher_id = teacher_id
        self.faculty_id = faculty_id

    @classmethod
    async def create_table(cls, conn: Any) -> None:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS Classes (
                ClassID VARCHAR(50) PRIMARY KEY,
                ClassName VARCHAR(100) NOT NULL,
                TeacherID VARCHAR(50) REFERENCES Teachers(TeacherID),
                FacultyID VARCHAR(50) REFERENCES Faculties(FacultyID),
                CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)