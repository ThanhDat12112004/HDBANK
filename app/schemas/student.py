from pydantic import BaseModel
from typing import Optional

class Student(BaseModel):
    studentid: str
    fullname: str
    dateofbirth: Optional[str] = None
    gender: Optional[str] = None
    address: Optional[str] = None
    email: str
    phone: Optional[str] = None
    classid: Optional[str] = None
    accountid: Optional[int] = None

    class Config:
        from_attributes = True