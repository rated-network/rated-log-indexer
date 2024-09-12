from typing import Tuple

from src.config.manager import ConfigurationManager
from src.indexers.offset_tracker.base import OffsetTracker
from src.indexers.offset_tracker.postgres import PostgresOffsetTracker
from src.indexers.offset_tracker.redis import RedisOffsetTracker


def get_offset_tracker(
    integration_prefix: str,
) -> Tuple[OffsetTracker, int]:
    config = ConfigurationManager.load_config().inputs
    input_config = next(
        (
            _input_config
            for _input_config in config
            if _input_config.integration_prefix == integration_prefix
        ),
        None,
    )

    if input_config is None:
        raise ValueError(
            f"Error with offset tracker configuration for integration_prefixed '{integration_prefix}', no configuration found"
        )

    offset_config = input_config.offset

    if offset_config.type == "postgres":
        return (
            PostgresOffsetTracker(offset_config, integration_prefix),
            offset_config.start_from,
        )
    elif offset_config.type == "redis":
        return (
            RedisOffsetTracker(offset_config, integration_prefix),
            offset_config.start_from,
        )
    else:
        raise ValueError(f"Unknown offset tracker type: {offset_config.type}")
