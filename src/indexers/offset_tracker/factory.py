from datetime import datetime
from typing import Union, Tuple

from config.config import ConfigurationManager
from indexers.offset_tracker.base import OffsetTracker
from indexers.offset_tracker.postgres import PostgresOffsetTracker
from indexers.offset_tracker.redis import RedisOffsetTracker


def get_offset_tracker() -> Tuple[OffsetTracker, Union[int, datetime]]:
    config = ConfigurationManager.get_config().offset
    if config.type == "postgres":
        return PostgresOffsetTracker(), config.start_from
    elif config.type == "redis":
        return RedisOffsetTracker(), config.start_from
    else:
        raise ValueError(f"Unknown offset tracker type: {config.type}")
