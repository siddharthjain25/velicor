import logging
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel

from app.models.user import UserCreate, UserInDB, Token, User, UserUpdate
from app.core.security import get_password_hash, verify_password, create_access_token
from app.db.mongo import mongo_manager
from jose import jwt, JWTError
from app.core.config import settings
from typing import Annotated

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Auth"])
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/v1/auth/token")


async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)]):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(
            token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM]
        )
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    user = await mongo_manager.db.users.find_one({"username": username})
    if user is None:
        raise credentials_exception
    return user


@router.post("/register", response_model=User)
async def register(user_in: UserCreate):
    existing_user = await mongo_manager.db.users.find_one(
        {"username": user_in.username}
    )
    if existing_user:
        raise HTTPException(status_code=400, detail="Username already registered")

    user_db = UserInDB(
        username=user_in.username,
        first_name=user_in.first_name,
        last_name=user_in.last_name,
        hashed_password=get_password_hash(user_in.password),
    )
    result = await mongo_manager.db.users.insert_one(user_db.model_dump())
    user_db_dict = user_db.model_dump()
    user_db_dict["_id"] = str(result.inserted_id)
    return user_db_dict


@router.post("/token", response_model=Token)
async def login(form_data: Annotated[OAuth2PasswordRequestForm, Depends()]):
    user = await mongo_manager.db.users.find_one({"username": form_data.username})
    if not user or not verify_password(form_data.password, user["hashed_password"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    from datetime import timedelta

    if user.get("two_factor_enabled"):
        # Generate short-lived temporary token for 2FA step
        temp_token = create_access_token(
            data={"sub": user["username"], "type": "2fa_temp"},
            expires_delta=timedelta(minutes=5),
        )
        return {
            "access_token": temp_token,
            "token_type": "2fa_temp",  # nosec B105
            "requires_2fa": True,
        }

    access_token = create_access_token(data={"sub": user["username"]})
    return {
        "access_token": access_token,
        "token_type": "bearer",  # nosec B105
        "requires_2fa": False,
    }


@router.get("/me", response_model=User)
async def read_users_me(current_user: Annotated[dict, Depends(get_current_user)]):
    current_user["_id"] = str(current_user["_id"])
    current_user["two_factor_backup_codes_count"] = len(
        current_user.get("two_factor_backup_codes", [])
    )
    return current_user


@router.put("/me", response_model=User)
async def update_user_me(
    user_update: UserUpdate, current_user: Annotated[dict, Depends(get_current_user)]
):
    update_data = user_update.model_dump(exclude_unset=True)
    if "password" in update_data:
        old_password = update_data.pop("old_password", None)
        if not old_password:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Old password is required to change password",
            )
        if not verify_password(old_password, current_user["hashed_password"]):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Incorrect old password",
            )
        update_data["hashed_password"] = get_password_hash(update_data.pop("password"))
    else:
        update_data.pop("old_password", None)

    if update_data:
        await mongo_manager.db.users.update_one(
            {"_id": current_user["_id"]}, {"$set": update_data}
        )

    updated_user = await mongo_manager.db.users.find_one({"_id": current_user["_id"]})
    updated_user["_id"] = str(updated_user["_id"])
    updated_user["two_factor_backup_codes_count"] = len(
        updated_user.get("two_factor_backup_codes", [])
    )
    return updated_user


@router.delete("/me", status_code=status.HTTP_204_NO_CONTENT)
async def delete_user_me(current_user: Annotated[dict, Depends(get_current_user)]):
    from app.db.postgres import pg_manager

    user_id = str(current_user["_id"])

    # 1. Find all services for this user
    services = await mongo_manager.db.services.find({"user_id": user_id}).to_list(None)

    # 2. Delete each service's Postgres table
    for service in services:
        try:
            await pg_manager.delete_table(service["name"])
        except Exception as e:
            logger.warning(
                f"Failed to delete table for service {service.get('name')}: {e}"
            )

    # 3. Delete all services from MongoDB
    await mongo_manager.db.services.delete_many({"user_id": user_id})

    # 4. Delete the user themselves
    await mongo_manager.db.users.delete_one({"_id": current_user["_id"]})

    return None


class Verify2FARequest(BaseModel):
    code: str


class TokenVerify2FARequest(BaseModel):
    temp_token: str
    code: str


class ResetPasswordRequest(BaseModel):
    username: str
    code: str
    new_password: str


@router.post("/2fa/setup")
async def setup_2fa(current_user: Annotated[dict, Depends(get_current_user)]):
    import pyotp

    # Generate random TOTP secret key
    secret = pyotp.random_base32()

    # Store temporary secret in user profile
    await mongo_manager.db.users.update_one(
        {"_id": current_user["_id"]}, {"$set": {"two_factor_secret": secret}}
    )

    provisioning_uri = pyotp.totp.TOTP(secret).provisioning_uri(
        name=current_user["username"], issuer_name="Velicor"
    )

    return {"secret": secret, "provisioning_uri": provisioning_uri}


@router.post("/2fa/enable")
async def enable_2fa(
    data: Verify2FARequest, current_user: Annotated[dict, Depends(get_current_user)]
):
    import pyotp
    import secrets
    import hashlib

    secret = current_user.get("two_factor_secret")
    if not secret:
        raise HTTPException(
            status_code=400,
            detail="2FA secret key has not been generated. Call setup first.",
        )

    totp = pyotp.TOTP(secret)
    if not totp.verify(data.code, valid_window=1):
        raise HTTPException(
            status_code=400, detail="Invalid verification code. Please try again."
        )

    # Generate 8 recovery/backup codes formatted as XXXX-XXXX
    plain_codes = []
    hashed_codes = []
    for _ in range(8):
        code = f"{secrets.token_hex(2).upper()}-{secrets.token_hex(2).upper()}"
        plain_codes.append(code)
        hashed_codes.append(hashlib.sha256(code.encode("utf-8")).hexdigest())

    await mongo_manager.db.users.update_one(
        {"_id": current_user["_id"]},
        {"$set": {"two_factor_enabled": True, "two_factor_backup_codes": hashed_codes}},
    )

    return {"status": "enabled", "backup_codes": plain_codes}


@router.post("/2fa/disable")
async def disable_2fa(
    data: Verify2FARequest, current_user: Annotated[dict, Depends(get_current_user)]
):
    import pyotp
    import hashlib

    if not current_user.get("two_factor_enabled"):
        raise HTTPException(status_code=400, detail="2FA is not currently enabled.")

    secret = current_user.get("two_factor_secret")
    if not secret:
        raise HTTPException(status_code=400, detail="2FA secret not found.")
    totp = pyotp.TOTP(secret)

    verified = False
    if totp.verify(data.code, valid_window=1):
        verified = True
    else:
        # Check if the code is a valid backup code
        hashed_input = hashlib.sha256(data.code.upper().encode("utf-8")).hexdigest()
        backup_codes = current_user.get("two_factor_backup_codes", [])
        if hashed_input in backup_codes:
            verified = True

    if not verified:
        raise HTTPException(
            status_code=400,
            detail="Invalid verification code or backup code. Please try again.",
        )

    await mongo_manager.db.users.update_one(
        {"_id": current_user["_id"]},
        {
            "$set": {
                "two_factor_enabled": False,
                "two_factor_secret": None,  # nosec B105
                "two_factor_backup_codes": [],
            }
        },
    )

    return {"status": "disabled"}


@router.post("/2fa/backup-codes/generate")
async def generate_new_backup_codes(
    data: Verify2FARequest, current_user: Annotated[dict, Depends(get_current_user)]
):
    import pyotp
    import secrets
    import hashlib

    if not current_user.get("two_factor_enabled"):
        raise HTTPException(status_code=400, detail="2FA is not currently enabled.")

    secret = current_user.get("two_factor_secret")
    if not secret:
        raise HTTPException(status_code=400, detail="2FA secret not found.")
    totp = pyotp.TOTP(secret)

    # Verify current 2FA OTP code first for security
    if not totp.verify(data.code, valid_window=1):
        raise HTTPException(
            status_code=400, detail="Invalid verification code. Please try again."
        )

    # Generate 8 new backup codes
    plain_codes = []
    hashed_codes = []
    for _ in range(8):
        code = f"{secrets.token_hex(2).upper()}-{secrets.token_hex(2).upper()}"
        plain_codes.append(code)
        hashed_codes.append(hashlib.sha256(code.encode("utf-8")).hexdigest())

    await mongo_manager.db.users.update_one(
        {"_id": current_user["_id"]},
        {"$set": {"two_factor_backup_codes": hashed_codes}},
    )

    return {"backup_codes": plain_codes}


@router.post("/token/verify-2fa", response_model=Token)
async def verify_2fa_login(data: TokenVerify2FARequest):
    import pyotp
    import hashlib

    try:
        payload = jwt.decode(
            data.temp_token,
            settings.JWT_SECRET_KEY,
            algorithms=[settings.JWT_ALGORITHM],
        )
        username: str = payload.get("sub")
        token_type: str = payload.get("type")
        if username is None or token_type != "2fa_temp":  # nosec B105
            raise HTTPException(
                status_code=401, detail="Invalid temporary session token"
            )
    except JWTError:
        raise HTTPException(
            status_code=401, detail="Invalid or expired temporary session token"
        )

    user = await mongo_manager.db.users.find_one({"username": username})
    if not user:
        raise HTTPException(status_code=401, detail="User not found")

    secret = user.get("two_factor_secret")
    if not secret:
        raise HTTPException(status_code=400, detail="2FA configuration is incomplete")

    totp = pyotp.TOTP(secret)
    verified = False

    # 1. Try verifying as TOTP
    if totp.verify(data.code, valid_window=1):
        verified = True
    else:
        # 2. Try verifying as backup code (atomic consume)
        hashed_input = hashlib.sha256(data.code.upper().encode("utf-8")).hexdigest()
        result = await mongo_manager.db.users.update_one(
            {"_id": user["_id"], "two_factor_backup_codes": hashed_input},
            {"$pull": {"two_factor_backup_codes": hashed_input}},
        )
        if result.modified_count > 0:
            verified = True

    if not verified:
        raise HTTPException(
            status_code=400,
            detail="Invalid verification code or backup code. Please try again.",
        )

    access_token = create_access_token(data={"sub": user["username"]})
    return {
        "access_token": access_token,
        "token_type": "bearer",  # nosec B105
        "requires_2fa": False,
    }


@router.post("/reset-password")
async def reset_password_with_2fa(data: ResetPasswordRequest):
    import pyotp
    import hashlib
    from app.core.security import get_password_hash

    user = await mongo_manager.db.users.find_one({"username": data.username})
    if not user:
        raise HTTPException(
            status_code=400, detail="User not found or invalid username."
        )

    if not user.get("two_factor_enabled"):
        raise HTTPException(
            status_code=400,
            detail="Self-service password reset is only available for accounts with active Two-Factor Authentication. Please contact your system administrator.",
        )

    secret = user.get("two_factor_secret")
    if not secret:
        raise HTTPException(status_code=400, detail="2FA configuration is incomplete.")

    totp = pyotp.TOTP(secret)
    verified = False
    hashed_backup_input = hashlib.sha256(data.code.upper().encode("utf-8")).hexdigest()

    # 1. Try verifying as TOTP
    if totp.verify(data.code, valid_window=1):
        verified = True
    else:
        # 2. Try verifying as backup code (atomic consume)
        result = await mongo_manager.db.users.update_one(
            {"_id": user["_id"], "two_factor_backup_codes": hashed_backup_input},
            {"$pull": {"two_factor_backup_codes": hashed_backup_input}},
        )
        if result.modified_count > 0:
            verified = True

    if not verified:
        raise HTTPException(
            status_code=400, detail="Invalid verification code or backup recovery code."
        )

    # Generate bcrypt hash for new password
    hashed_pwd = get_password_hash(data.new_password)

    # We only update the password now because the backup code has already been pulled atomically
    await mongo_manager.db.users.update_one(
        {"_id": user["_id"]}, {"$set": {"hashed_password": hashed_pwd}}
    )

    return {"status": "password_reset"}
