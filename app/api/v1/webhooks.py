from fastapi import APIRouter, Depends, HTTPException, status
from app.models.service import WebhookConfig
from app.api.v1.auth import get_current_user
from app.db.mongo import mongo_manager
from app.api.v1.endpoints import invalidate_service_cache
from typing import List, Annotated

router = APIRouter(tags=["Webhooks"])

@router.get("/", response_model=List[WebhookConfig])
async def list_webhooks(current_user: Annotated[dict, Depends(get_current_user)]):
    user = await mongo_manager.db.users.find_one({"_id": current_user["_id"]})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user.get("webhooks", [])

@router.post("/", response_model=WebhookConfig)
async def add_webhook(
    webhook: dict,
    current_user: Annotated[dict, Depends(get_current_user)]
):
    new_webhook = WebhookConfig(**webhook)
    await mongo_manager.db.users.update_one(
        {"_id": current_user["_id"]},
        {"$push": {"webhooks": new_webhook.model_dump()}}
    )
    # Clear ingestion cache to reflect changes immediately
    await invalidate_service_cache()
    return new_webhook

@router.delete("/{webhook_id}")
async def delete_webhook(
    webhook_id: str,
    current_user: Annotated[dict, Depends(get_current_user)]
):
    result = await mongo_manager.db.users.update_one(
        {"_id": current_user["_id"]},
        {"$pull": {"webhooks": {"id": webhook_id}}}
    )
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Webhook not found")
    # Clear ingestion cache to reflect changes immediately
    await invalidate_service_cache()
    return {"status": "deleted"}

@router.patch("/{webhook_id}")
async def update_webhook(
    webhook_id: str,
    webhook_update: dict,
    current_user: Annotated[dict, Depends(get_current_user)]
):
    update_fields = {f"webhooks.$.{k}": v for k, v in webhook_update.items() if k != "id"}
    result = await mongo_manager.db.users.update_one(
        {"_id": current_user["_id"], "webhooks.id": webhook_id},
        {"$set": update_fields}
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Webhook not found")
    # Clear ingestion cache to reflect changes immediately
    await invalidate_service_cache()
    return {"status": "updated"}
