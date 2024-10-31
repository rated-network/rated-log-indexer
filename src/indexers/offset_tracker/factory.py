from typing import Tuple
from collections import defaultdict

from src.config.manager import ConfigurationManager
from src.indexers.offset_tracker.base import OffsetTracker
from src.indexers.offset_tracker.postgres import PostgresOffsetTracker
from src.indexers.offset_tracker.rated import RatedAPIOffsetTracker
from src.indexers.offset_tracker.redis import RedisOffsetTracker


def get_offset_tracker(
    integration_prefix: str, config_index: int = 0
) -> Tuple[OffsetTracker, int]:
    """
    Retrieves the appropriate OffsetTracker and start_from value for a given integration prefix and configuration index.

    This function handles scenarios where multiple configurations may exist for the same integration prefix.
    It performs the following steps:
    1. Loads all input configurations from the ConfigurationManager.
    2. Groups configurations by their integration prefix.
    3. Retrieves all configurations matching the provided integration prefix.
    4. Selects the specific configuration based on the config_index.
    5. Creates and returns the appropriate OffsetTracker (PostgresOffsetTracker or RedisOffsetTracker)
       along with its start_from value.

    If multiple configurations exist for the same prefix, the function appends the config_index
    to the integration prefix to ensure unique identification for each configuration.

    Args:
        integration_prefix (str): The integration prefix to look up in the configuration.
        config_index (int, optional): The index of the configuration to use when multiple
                                      configurations exist for the same prefix. Defaults to 0.

    Returns:
        Tuple[OffsetTracker, int]: A tuple containing:
            - An instance of the appropriate OffsetTracker subclass (PostgresOffsetTracker or RedisOffsetTracker).
            - The start_from value from the configuration.

    Raises:
        ValueError: In the following cases:
            - No configuration found for the given integration prefix.
            - The provided config_index is out of range for the given integration prefix.
            - Unknown offset tracker type specified in the configuration.

    Example:
        offset_tracker, start_from = get_offset_tracker("my_integration")
        # Or for a specific configuration when multiple exist:
        offset_tracker, start_from = get_offset_tracker("my_integration", config_index=1)
    """
    config = ConfigurationManager.load_config().inputs

    grouped_configs = defaultdict(list)
    for input_config in config:
        grouped_configs[input_config.integration_prefix].append(input_config)

    matching_configs = grouped_configs.get(integration_prefix, [])
    if not matching_configs:
        raise ValueError(
            f"No configuration found for integration prefix '{integration_prefix}'"
        )

    if config_index >= len(matching_configs):
        raise ValueError(
            f"Config index {config_index} is out of range. Only {len(matching_configs)} configurations found for prefix '{integration_prefix}'"
        )

    input_config = matching_configs[config_index]
    offset_config = input_config.offset

    final_integration_prefix = (
        f"{integration_prefix}_{config_index}"
        if len(matching_configs) > 1
        else integration_prefix
    )

    if offset_config.type == "postgres":
        return (
            PostgresOffsetTracker(offset_config, final_integration_prefix),
            offset_config.start_from,
        )
    elif offset_config.type == "redis":
        return (
            RedisOffsetTracker(offset_config, final_integration_prefix),
            offset_config.start_from,
        )
    elif offset_config.type == "slaos":
        return (
            RatedAPIOffsetTracker(offset_config, final_integration_prefix),
            offset_config.start_from,
        )
    else:
        raise ValueError(f"Unknown offset tracker type: {offset_config.type}")
