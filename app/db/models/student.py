from app.db.base import BaseModel
from typing import Optional, Any
from datetime import datetime

class Student(BaseModel):
    def __init__(
        self,
        student_id: str,
        full_name: str,
        email: str,
        date_of_birth: Optional[datetime] = None,
        gender: Optional[str] = None,
        address: Optional[str] = None,
        phone: Optional[str] = None,
        class_id: Optional[str] = None,
        account_id: Optional[int] = None,
    ):
        self.student_id = student_id
        self.full_name = full_name
        self.email = email
        self.date_of_birth = date_of_birth
        self.gender = gender
        self.address = address
        self.phone = phone
        self.class_id = class_id
        self.account_id = account_id

    @classmethod
    async def create_table(cls, conn: Any) -> None:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS Students (
                StudentID VARCHAR(50) PRIMARY KEY,
                FullName VARCHAR(100) NOT NULL,
                DateOfBirth DATE,
                Gender VARCHAR(10),
                Address TEXT,
                Email VARCHAR(100) NOT NULL,
                Phone VARCHAR(20),
                ClassID VARCHAR(50) REFERENCES Classes(ClassID),
                AccountID INTEGER REFERENCES Accounts(AccountID),
                CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP

            )
        """)