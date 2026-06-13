from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
from app.models.service import WebhookConfig


class UserBase(BaseModel):
    username: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None


class UserCreate(UserBase):
    password: str


class UserUpdate(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    password: Optional[str] = None
    old_password: Optional[str] = None


class UserInDB(UserBase):
    hashed_password: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    webhooks: List[WebhookConfig] = []
    two_factor_secret: Optional[str] = None
    two_factor_enabled: bool = False
    two_factor_backup_codes: List[str] = []


class User(UserBase):
    id: str = Field(alias="_id")
    created_at: datetime
    webhooks: List[WebhookConfig] = []
    two_factor_enabled: bool = False
    two_factor_backup_codes_count: int = 0

    class Config:
        populate_by_name = True


class Token(BaseModel):
    access_token: str
    token_type: str
    requires_2fa: bool = False


class TokenData(BaseModel):
    username: Optional[str] = None
