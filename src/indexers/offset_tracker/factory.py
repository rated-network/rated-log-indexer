from datetime import datetime
from typing import Union, Tuple

from src.config.manager import ConfigurationManager
from src.indexers.offset_tracker.base import OffsetTracker
from src.indexers.offset_tracker.postgres import PostgresOffsetTracker
from src.indexers.offset_tracker.redis import RedisOffsetTracker


def get_offset_tracker() -> Tuple[OffsetTracker, Union[int, datetime]]:
    # TODO: Fix this to automatically get it from the config
    config = ConfigurationManager.load_config().inputs[0].offset
    if config.type == "postgres":
        return PostgresOffsetTracker(), config.start_from
    elif config.type == "redis":
        return RedisOffsetTracker(), config.start_from
    else:
        raise ValueError(f"Unknown offset tracker type: {config.type}")
