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
            pool_kwargs = {}
            if settings.SERVERLESS_MODE:
                pool_kwargs = {"maxPoolSize": 10, "minPoolSize": 0}

            self.client = AsyncIOMotorClient(settings.MONGO_URI, **pool_kwargs)
            self.db = self.client[settings.MONGO_DB_NAME]

            try:
                # Issue a ping to verify the connection
                await self.client.admin.command("ping")
                logger.info(f"Connected to MongoDB: {settings.MONGO_DB_NAME}")
            except Exception as e:
                logger.exception(f"Failed to connect to MongoDB: {e}")
                raise

    def get_db(self):
        if not self.client:
            logger.warning("get_db() called before MongoDB client was initialized.")
        return self.db

    def disconnect(self):
        if self.client:
            self.client.close()
            self.client = None
            self.db = None
            logger.info("Disconnected from MongoDB")


mongo_manager = MongoManager()
