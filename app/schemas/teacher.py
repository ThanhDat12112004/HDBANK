from pydantic import BaseModel
from typing import Optional

class Teacher(BaseModel):
    teacherId: str
    fullName: str
    email: str
    phone: Optional[str] = None

    class Config:
        from_attributes = True