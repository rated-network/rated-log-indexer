from typing import Optional, Any

from pydantic import BaseModel
import redis


class RedisConfig(BaseModel):
    host: str
    port: int
    db: int


class RedisClient:
    """
    Handles connections and operations with Redis
    """

    def __init__(self, config: RedisConfig):
        self.config = config
        self.client: Optional[redis.Redis] = None
        self.connect()

    def connect(self) -> None:
        if self.client is None:
            self.client = redis.Redis(
                host=self.config.host,
                port=self.config.port,
                db=self.config.db,
                decode_responses=True,
            )

    def close(self) -> None:
        if self.client:
            self.client.close()
            self.client = None

    def get(self, key: str) -> Optional[Any]:
        if self.client:
            return self.client.get(key)
        else:
            return None

    def set(self, key: str, value: str) -> None:
        if self.client:
            self.client.set(key, value)
        else:
            return None
