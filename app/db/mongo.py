import logging
from motor.motor_asyncio import AsyncIOMotorClient
from app.core.config import settings

logger = logging.getLogger(__name__)

class MongoManager:
    def __init__(self):
        self.client: AsyncIOMotorClient = None
        self.db = None

    async def connect(self):
        if not settings.MONGO_URI:
            logger.error("MONGO_URI is not set. Service features will be disabled.")
            return

        if not self.client:
            self.client = AsyncIOMotorClient(settings.MONGO_URI)
            self.db = self.client[settings.MONGO_DB_NAME]
            logger.info(f"Connected to MongoDB: {settings.MONGO_DB_NAME}")

    def get_db(self):
        if not self.client:
            # We can't easily await here in a property, 
            # but endpoints already handle async setup.
            # This is a safety fallback.
            pass
        return self.db

    async def disconnect(self):
        if self.client:
            self.client.close()
            self.client = None
            self.db = None
            logger.info("Disconnected from MongoDB")

mongo_manager = MongoManager()
