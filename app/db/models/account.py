from app.db.base import BaseModel
from typing import Optional, Any
from datetime import datetime

class Account(BaseModel):
    def __init__(
        self,
        accountid: int,
        username: str,
        passwordhash: str,
        role: str,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
    ):
        self.accountid = accountid
        self.username = username
        self.passwordhash = passwordhash
        self.role = role
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()

    @classmethod
    async def create_table(cls, conn: Any) -> None:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS Accounts (
                accountid SERIAL PRIMARY KEY,
                username VARCHAR(255) NOT NULL UNIQUE,
                passwordhash VARCHAR(255) NOT NULL,
                role VARCHAR(50) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    @classmethod
    async def drop_table(cls, conn: Any) -> None:
        await conn.execute("DROP TABLE IF EXISTS Accounts")