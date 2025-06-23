from app.db.base import BaseModel
from typing import Optional, Any
from datetime import datetime

class Grade(BaseModel):
    def __init__(
        self,
        student_id: str,
        subject_id: str,
        score: Optional[float] = None,
        semester: Optional[str] = None,
    ):
        self.student_id = student_id
        self.subject_id = subject_id
        self.score = score
        self.semester = semester

    @classmethod
    async def create_table(cls, conn: Any) -> None:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS Grades (
                GradeID SERIAL PRIMARY KEY,
                StudentID VARCHAR(50) REFERENCES Students(StudentID),
                SubjectID VARCHAR(50) REFERENCES Subjects(SubjectID),
                Score DECIMAL(4,2),  
                Semester VARCHAR(20),
                CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)