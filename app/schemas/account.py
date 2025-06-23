from pydantic import BaseModel
from typing import Optional

class AccountCreate(BaseModel):
    username: str
    password: str
    role: str = "user"


class AccountUpdate(BaseModel):
    username: Optional[str] = None
    password: Optional[str] = None
    role: Optional[str] = None

class Account(BaseModel):
    accountid: int
    username: str
    role: str
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    class Config:
        from_attributes = True