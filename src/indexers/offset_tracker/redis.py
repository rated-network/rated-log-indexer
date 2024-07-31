from typing import Union
from datetime import datetime

from pydantic import StrictInt

from src.clients.redis import RedisConfig, RedisClient
from src.indexers.offset_tracker.base import OffsetTracker


class RedisOffsetTracker(OffsetTracker):
    def __init__(self):
        super().__init__()
        if self.config.type != "redis":
            raise ValueError(
                "Offset tracker type is not set to 'redis' in the configuration"
            )

        redis_config = RedisConfig(
            host=self.config.redis.host,
            port=self.config.redis.port,
            db=self.config.redis.db,
        )
        self.client = RedisClient(redis_config)
        self.key = self.config.redis.key

    def get_current_offset(self) -> Union[StrictInt, datetime]:
        value = self.client.get(self.key)
        if value is None:
            return self.start_from

        if isinstance(self.start_from, int):
            return int(value)
        else:
            return datetime.fromisoformat(value)

    def update_offset(self, offset: Union[StrictInt, datetime]) -> None:
        if isinstance(offset, datetime):
            offset = offset.isoformat()
        self.client.set(self.key, str(offset))
