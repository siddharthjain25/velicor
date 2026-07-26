from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, model_validator


class WebhookConfig(BaseModel):
    id: str = Field(default_factory=lambda: datetime.now().strftime("%Y%m%d%H%M%S%f"))
    url: str
    levels: list[str] = ["ERROR", "FATAL"]
    keywords: list[str] | None = None
    enabled: bool = True
    services: list[str] | None = None


class ServiceBase(BaseModel):
    name: str
    retention_days: int = 30
    retention_minutes: int = 43200
    webhooks: list[WebhookConfig] = []
    custom_severities: list[str] = []

    @model_validator(mode="before")
    @classmethod
    def sync_retention_before(cls, data: Any) -> Any:
        if isinstance(data, dict):
            days = data.get("retention_days")
            minutes = data.get("retention_minutes")
            if minutes is not None:
                data["retention_days"] = max(1, minutes // 1440)
            elif days is not None:
                data["retention_minutes"] = days * 1440
        return data


class ServiceCreate(ServiceBase):
    pass


class ServiceUpdate(BaseModel):
    retention_days: int | None = None
    retention_minutes: int | None = None
    webhooks: list[WebhookConfig] | None = None
    custom_severities: list[str] | None = None

    @model_validator(mode="before")
    @classmethod
    def sync_retention_before(cls, data: Any) -> Any:
        if isinstance(data, dict):
            days = data.get("retention_days")
            minutes = data.get("retention_minutes")
            if minutes is not None:
                data["retention_days"] = max(1, minutes // 1440)
            elif days is not None:
                data["retention_minutes"] = days * 1440
        return data


class ServiceInDB(ServiceBase):
    user_id: str
    secret_key: str
    created_at: datetime = Field(default_factory=datetime.utcnow)


class Service(ServiceBase):
    id: str = Field(alias="_id")
    secret_key: str
    created_at: datetime

    class Config:
        populate_by_name = True
