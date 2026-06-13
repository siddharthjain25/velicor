from fastapi import APIRouter, Depends, HTTPException, status, Header
from app.models.service import ServiceCreate, ServiceInDB, Service, ServiceUpdate
from app.api.v1.auth import get_current_user
from app.db.mongo import mongo_manager
from app.core.config import settings
import secrets
from typing import List, Annotated, Optional

from app.db.postgres import pg_manager
from app.api.v1.endpoints import invalidate_service_cache

router = APIRouter(tags=["Services"])

@router.delete("/{service_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_service(
    service_id: str,
    current_user: Annotated[dict, Depends(get_current_user)],
    x_2fa_code: Annotated[Optional[str], Header(alias="X-2FA-Code")] = None,
    code: Optional[str] = None
):
    from bson import ObjectId
    try:
        obj_id = ObjectId(service_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid service ID")

    # Verify 2FA if enabled
    if current_user.get("two_factor_enabled"):
        verification_code = x_2fa_code or code
        if not verification_code:
            raise HTTPException(
                status_code=403, 
                detail="Two-factor authentication code is required to delete this service. Please provide it in the 'X-2FA-Code' header."
            )
            
        import pyotp
        import hashlib
        secret = current_user.get("two_factor_secret")
        if not secret:
            raise HTTPException(status_code=500, detail="2FA state is corrupted")
            
        totp = pyotp.TOTP(secret)
        verified = False
        
        # 1. Try verifying as TOTP
        if totp.verify(verification_code, valid_window=1):
            verified = True
        else:
            # 2. Try verifying as backup code (atomic consume)
            hashed_input = hashlib.sha256(verification_code.upper().encode('utf-8')).hexdigest()
            result = await mongo_manager.db.users.update_one(
                {"_id": current_user["_id"], "two_factor_backup_codes": hashed_input},
                {"$pull": {"two_factor_backup_codes": hashed_input}}
            )
            if result.modified_count > 0:
                verified = True
                
        if not verified:
            raise HTTPException(status_code=400, detail="Invalid 2FA verification code or backup code")

    # Find the service and verify ownership
    service = await mongo_manager.db.services.find_one({
        "_id": obj_id,
        "user_id": str(current_user["_id"])
    })
    
    if not service:
        raise HTTPException(status_code=404, detail="Service not found")
    
    # Invalidate cache before deletion
    await invalidate_service_cache(service_id)

    # 1. Delete from MongoDB
    await mongo_manager.db.services.delete_one({"_id": obj_id})
    
    # 2. Drop Postgres Table
    await pg_manager.delete_table(service["name"])
    
    return None

@router.post("/{service_id}/reset-key", response_model=Service)
async def reset_service_key(
    service_id: str,
    current_user: Annotated[dict, Depends(get_current_user)]
):
    from bson import ObjectId
    try:
        obj_id = ObjectId(service_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid service ID")

    # Find the service and verify ownership
    service = await mongo_manager.db.services.find_one({
        "_id": obj_id,
        "user_id": str(current_user["_id"])
    })
    
    if not service:
        raise HTTPException(status_code=404, detail="Service not found")
    
    # Invalidate cache before key reset
    await invalidate_service_cache(service_id)

    # Generate new secret key
    new_secret_key = secrets.token_urlsafe(32)
    
    # Update in MongoDB
    await mongo_manager.db.services.update_one(
        {"_id": obj_id},
        {"$set": {"secret_key": new_secret_key}}
    )
    
    # Get updated service
    updated_service = await mongo_manager.db.services.find_one({"_id": obj_id})
    updated_service["_id"] = str(updated_service["_id"])
    return updated_service

@router.post("/", response_model=Service)
async def create_service(
    service_in: ServiceCreate,
    current_user: Annotated[dict, Depends(get_current_user)]
):
    # Check if service name already exists for this user
    existing = await mongo_manager.db.services.find_one({
        "user_id": str(current_user["_id"]),
        "name": service_in.name
    })
    if existing:
        raise HTTPException(status_code=400, detail="Service name already exists")
    
    secret_key = secrets.token_urlsafe(32)
    service_db = ServiceInDB(
        name=service_in.name,
        user_id=str(current_user["_id"]),
        secret_key=secret_key
    )
    result = await mongo_manager.db.services.insert_one(service_db.model_dump())
    
    res = service_db.model_dump()
    res["_id"] = str(result.inserted_id)
    return res

@router.get("/", response_model=List[Service])
async def list_services(current_user: Annotated[dict, Depends(get_current_user)]):
    services = await mongo_manager.db.services.find({"user_id": str(current_user["_id"])}).to_list(100)
    for s in services:
        s["_id"] = str(s["_id"])
    return services

@router.patch("/{service_id}", response_model=Service)
async def update_service(
    service_id: str,
    service_update: ServiceUpdate,
    current_user: Annotated[dict, Depends(get_current_user)]
):
    from bson import ObjectId
    try:
        obj_id = ObjectId(service_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid service ID")

    # Verify ownership
    existing = await mongo_manager.db.services.find_one({
        "_id": obj_id,
        "user_id": str(current_user["_id"])
    })
    if not existing:
        raise HTTPException(status_code=404, detail="Service not found")

    update_data = service_update.model_dump(exclude_unset=True)
    if update_data:
        await mongo_manager.db.services.update_one(
            {"_id": obj_id},
            {"$set": update_data}
        )
    
    # Invalidate cache after update
    await invalidate_service_cache(service_id)

    updated = await mongo_manager.db.services.find_one({"_id": obj_id})
    updated["_id"] = str(updated["_id"])
    return updated

@router.get("/{service_id}/stats")
async def get_service_stats(
    service_id: str,
    current_user: Annotated[dict, Depends(get_current_user)],
    interval_hours: int = 24
):
    from bson import ObjectId
    try:
        obj_id = ObjectId(service_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid service ID")

    service = await mongo_manager.db.services.find_one({
        "_id": obj_id,
        "user_id": str(current_user["_id"])
    })
    
    if not service:
        raise HTTPException(status_code=404, detail="Service not found")
    
    return await pg_manager.get_stats(service["name"], interval_hours)

@router.post("/{service_id}/webhooks")
async def add_webhook(
    service_id: str,
    webhook: dict,
    current_user: Annotated[dict, Depends(get_current_user)]
):
    from bson import ObjectId
    try:
        obj_id = ObjectId(service_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid service ID")

    from app.models.service import WebhookConfig
    new_webhook = WebhookConfig(**webhook)

    result = await mongo_manager.db.services.update_one(
        {"_id": obj_id, "user_id": str(current_user["_id"])},
        {"$push": {"webhooks": new_webhook.model_dump()}}
    )
    
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Service not found")
    
    # Invalidate cache after adding webhook
    await invalidate_service_cache(service_id)
        
    return new_webhook

@router.delete("/{service_id}/webhooks/{webhook_id}")
async def delete_webhook(
    service_id: str,
    webhook_id: str,
    current_user: Annotated[dict, Depends(get_current_user)]
):
    from bson import ObjectId
    try:
        obj_id = ObjectId(service_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid service ID")

    result = await mongo_manager.db.services.update_one(
        {"_id": obj_id, "user_id": str(current_user["_id"])},
        {"$pull": {"webhooks": {"id": webhook_id}}}
    )
    
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Service not found")
    
    # Invalidate cache after deleting webhook
    await invalidate_service_cache(service_id)
        
    return {"status": "deleted"}

@router.patch("/{service_id}/webhooks/{webhook_id}")
async def update_webhook(
    service_id: str,
    webhook_id: str,
    webhook_update: dict,
    current_user: Annotated[dict, Depends(get_current_user)]
):
    from bson import ObjectId
    try:
        obj_id = ObjectId(service_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid service ID")

    # We use positional operator $ to update the specific element in the list
    update_fields = {f"webhooks.$.{k}": v for k, v in webhook_update.items() if k != "id"}
    
    result = await mongo_manager.db.services.update_one(
        {"_id": obj_id, "user_id": str(current_user["_id"]), "webhooks.id": webhook_id},
        {"$set": update_fields}
    )
    
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Service or Webhook not found")
    
    # Invalidate cache after updating webhook
    await invalidate_service_cache(service_id)
        
    return {"status": "updated"}

@router.post("/purge-all", status_code=status.HTTP_200_OK)
async def trigger_global_purge(
    authorization: Annotated[Optional[str], Header()] = None,
    x_cron_secret: Annotated[Optional[str], Header()] = None
):
    # 1. Check for Cron Secret (System-wide purge)
    if settings.CRON_SECRET and x_cron_secret == settings.CRON_SECRET:
        all_services = await mongo_manager.db.services.find({}).to_list(None)
        count = 0
        from app.models.service import WebhookConfig
        from app.services.notifier import trigger_retention_webhooks
        for service in all_services:
            retention = service.get("retention_days", 30)
            deleted_count = await pg_manager.purge_old_logs(service["name"], retention)
            
            webhooks_data = service.get("webhooks", [])
            if webhooks_data:
                webhooks = [WebhookConfig(**w) for w in webhooks_data]
                await trigger_retention_webhooks(webhooks, service["name"], retention, deleted_count)
            count += 1
        return {"message": f"System-wide purge completed for {count} services"}

    # 2. Check for User Auth (User-specific purge)
    if not authorization:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    # We need to manually verify token here since we made auth optional in the signature
    from app.api.v1.auth import get_current_user
    try:
        token = authorization.split(" ")[1] if " " in authorization else authorization
        current_user = await get_current_user(token)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    user_services = await mongo_manager.db.services.find({"user_id": str(current_user["_id"])}).to_list(None)
    count = 0
    from app.models.service import WebhookConfig
    from app.services.notifier import trigger_retention_webhooks
    for service in user_services:
        retention = service.get("retention_days", 30)
        deleted_count = await pg_manager.purge_old_logs(service["name"], retention)
        
        webhooks_data = service.get("webhooks", [])
        if webhooks_data:
            webhooks = [WebhookConfig(**w) for w in webhooks_data]
            await trigger_retention_webhooks(webhooks, service["name"], retention, deleted_count)
        count += 1
        
    return {"message": f"Purge completed for {count} services"}
