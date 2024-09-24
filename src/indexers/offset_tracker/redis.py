from typing import cast

from pydantic import StrictInt, StrictStr

from src.config.models.offset import OffsetYamlConfig
from src.clients.redis import RedisConfig, RedisClient
from src.indexers.offset_tracker.base import OffsetTracker


class RedisOffsetTracker(OffsetTracker):
    def __init__(self, config: OffsetYamlConfig, integration_prefix: StrictStr):
        super().__init__(config=config, integration_prefix=integration_prefix)
        self.config = config
        self.integration_prefix = integration_prefix

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
        self.key = f"{self.integration_prefix}:{cast(str, self.config.redis.key)}"
        self._override_applied = False

    def get_current_offset(self) -> StrictInt:
        value = self.client.get(self.key)

        if self.config.override_start_from and not self._override_applied:
            self._override_applied = True
            self.update_offset(self.config.start_from)
            value = self.client.get(self.key)

        if value is None:
            return self.config.start_from

        return StrictInt(value)

    def update_offset(self, offset: StrictInt) -> None:
        self.client.set(self.key, str(offset))
