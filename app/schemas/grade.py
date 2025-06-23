from pydantic import BaseModel
from typing import Optional

class Grade(BaseModel):
    gradeId: Optional[int] = None
    studentId: str
    subjectId: str
    score: Optional[float] = None
    semester: Optional[str] = None

    class Config:
        from_attributes = True