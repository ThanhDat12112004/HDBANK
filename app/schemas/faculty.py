from pydantic import BaseModel

class Faculty(BaseModel):
    facultyId: str
    facultyName: str

    class Config:
        from_attributes = True