from typing import Union, cast
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

        assert self.config.redis is not None

        redis_config = RedisConfig(
            host=self.config.redis.host,
            port=self.config.redis.port,
            db=self.config.redis.db,
        )
        self.client = RedisClient(redis_config)
        self.key = cast(str, self.config.redis.key)
        self._override_applied = False

    def get_current_offset(self) -> Union[StrictInt, datetime]:
        value = self.client.get(self.key)

        if self.config.override_start_from and not self._override_applied:
            self._override_applied = True
            value = self.client.get(self.start_from)

        if value is None:
            return self.start_from

        if isinstance(self.start_from, int):
            return StrictInt(value)
        else:
            return datetime.fromisoformat(value)

    def update_offset(self, offset: Union[StrictInt, datetime]) -> None:
        if isinstance(offset, datetime):
            converted_offset = offset.isoformat()
            self.client.set(self.key, str(converted_offset))
        else:
            self.client.set(self.key, str(offset))
