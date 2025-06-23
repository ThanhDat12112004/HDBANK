from typing import Any
from datetime import datetime

class BaseModel:
    created_at: datetime
    updated_at: datetime

    @classmethod
    async def create_table(cls, conn: Any) -> None:
        """Create table if it doesn't exist"""
        pass  # Implement if needed

    @classmethod
    async def drop_table(cls, conn: Any) -> None:
        """Drop table if it exists"""
        pass  # Implement if needed