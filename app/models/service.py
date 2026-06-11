from pydantic import BaseModel, Field
from typing import Optional, List
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
    webhooks: List[WebhookConfig] = []

class ServiceCreate(ServiceBase):
    pass

class ServiceUpdate(BaseModel):
    retention_days: Optional[int] = None
    webhooks: Optional[List[WebhookConfig]] = None

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
