from pydantic import BaseModel
from typing import Optional

class Subject(BaseModel):
    subjectId: str
    subjectName: str
    credits: Optional[int] = None

    class Config:
        from_attributes = True