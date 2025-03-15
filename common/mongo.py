import os

from motor.motor_asyncio import AsyncIOMotorClient


_connection_url = os.environ.get("MONGO_CONNECTION_URL")
_connection_url = _connection_url or "mongodb://localhost:27018"

mongo_client = AsyncIOMotorClient(_connection_url)
