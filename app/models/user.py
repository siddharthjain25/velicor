from datetime import datetime

from pydantic import BaseModel, Field

from app.models.service import WebhookConfig


class UserBase(BaseModel):
    username: str
    first_name: str | None = None
    last_name: str | None = None


class UserCreate(UserBase):
    password: str


class UserUpdate(BaseModel):
    first_name: str | None = None
    last_name: str | None = None
    password: str | None = None
    old_password: str | None = None


class UserInDB(UserBase):
    hashed_password: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    webhooks: list[WebhookConfig] = []
    two_factor_secret: str | None = None
    two_factor_enabled: bool = False
    two_factor_backup_codes: list[str] = []


class User(UserBase):
    id: str = Field(alias="_id")
    created_at: datetime
    webhooks: list[WebhookConfig] = []
    two_factor_enabled: bool = False
    two_factor_backup_codes_count: int = 0

    class Config:
        populate_by_name = True


class Token(BaseModel):
    access_token: str
    token_type: str
    requires_2fa: bool = False


class TokenData(BaseModel):
    username: str | None = None
