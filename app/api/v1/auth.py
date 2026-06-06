from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from app.models.user import UserCreate, UserInDB, Token, User, UserUpdate
from app.core.security import get_password_hash, verify_password, create_access_token
from app.db.mongo import mongo_manager
from jose import jwt, JWTError
from app.core.config import settings
from typing import Annotated

router = APIRouter(tags=["Auth"])
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/v1/auth/token")

async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)]):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM])
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
    existing_user = await mongo_manager.db.users.find_one({"username": user_in.username})
    if existing_user:
        raise HTTPException(status_code=400, detail="Username already registered")
    
    user_db = UserInDB(
        username=user_in.username,
        first_name=user_in.first_name,
        last_name=user_in.last_name,
        hashed_password=get_password_hash(user_in.password)
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
    
    access_token = create_access_token(data={"sub": user["username"]})
    return {"access_token": access_token, "token_type": "bearer"}

@router.get("/me", response_model=User)
async def read_users_me(current_user: Annotated[dict, Depends(get_current_user)]):
    current_user["_id"] = str(current_user["_id"])
    return current_user

@router.put("/me", response_model=User)
async def update_user_me(
    user_update: UserUpdate, 
    current_user: Annotated[dict, Depends(get_current_user)]
):
    update_data = user_update.model_dump(exclude_unset=True)
    if "password" in update_data:
        update_data["hashed_password"] = get_password_hash(update_data.pop("password"))
    
    if update_data:
        await mongo_manager.db.users.update_one(
            {"_id": current_user["_id"]},
            {"$set": update_data}
        )
    
    updated_user = await mongo_manager.db.users.find_one({"_id": current_user["_id"]})
    updated_user["_id"] = str(updated_user["_id"])
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
        except Exception:
            # Continue even if table deletion fails
            pass
            
    # 3. Delete all services from MongoDB
    await mongo_manager.db.services.delete_many({"user_id": user_id})
    
    # 4. Delete the user themselves
    await mongo_manager.db.users.delete_one({"_id": current_user["_id"]})
    
    return None
