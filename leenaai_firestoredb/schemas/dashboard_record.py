from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel


class DashboardRecord(BaseModel):
    client_id: str
    user_id: str
    phone_number: Optional[str] = None
    last_action: Optional[str] = None
    requirement_name: str
    updated_at: datetime
    messages_count: int
    name: Optional[str] = ""
    unread_messages_count: int = 0
    score: float = 0
