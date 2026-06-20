from pydantic import BaseModel, Field, model_validator
from typing import Optional, List, Any
from datetime import datetime


class WebhookConfig(BaseModel):
    id: str = Field(default_factory=lambda: datetime.now().strftime("%Y%m%d%H%M%S%f"))
    url: str
    levels: List[str] = ["ERROR", "FATAL"]
    keywords: Optional[List[str]] = None
    enabled: bool = True
    services: Optional[List[str]] = None


class ServiceBase(BaseModel):
    name: str
    retention_days: int = 30
    retention_minutes: int = 43200
    webhooks: List[WebhookConfig] = []
    custom_severities: List[str] = []

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
    retention_days: Optional[int] = None
    retention_minutes: Optional[int] = None
    webhooks: Optional[List[WebhookConfig]] = None
    custom_severities: Optional[List[str]] = None

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
