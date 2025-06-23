from pydantic import BaseModel
from typing import Optional

class Class(BaseModel):
    classid: str
    classname: str
    teacherid: Optional[str] = None
    facultyid: Optional[str] = None

    class Config:
        from_attributes = True