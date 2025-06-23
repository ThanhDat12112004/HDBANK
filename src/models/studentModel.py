from pydantic import BaseModel
from typing import Optional

class Student(BaseModel):
    mssv: Optional[int] = None
    hovaten: str
    tuoi: int