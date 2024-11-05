from copy import deepcopy
from datetime import UTC, datetime

import pytest
from unittest.mock import patch
from src.config.manager import ConfigurationManager, RatedIndexerYamlConfig
from src.indexers.offset_tracker.factory import get_offset_tracker
from src.indexers.offset_tracker.postgres import PostgresOffsetTracker
from src.indexers.offset_tracker.rated import RatedAPIOffsetTracker
from src.indexers.offset_tracker.redis import RedisOffsetTracker


@patch.object(ConfigurationManager, "load_config")
def test_get_offset_tracker_no_duplicates(mock_load_config, valid_config_dict):
    config_dict = deepcopy(valid_config_dict)
    config_dict["inputs"][0]["slaos_key"] = "prefix1"
    config = RatedIndexerYamlConfig(**config_dict)
    mock_load_config.return_value = config

    tracker, start_from = get_offset_tracker("prefix1")

    assert isinstance(tracker, PostgresOffsetTracker)
    assert tracker.slaos_key == "prefix1"
    assert start_from == 123456789


@patch.object(ConfigurationManager, "load_config")
def test_get_offset_tracker_with_duplicates(mock_load_config, valid_config_dict):
    config_dict = deepcopy(valid_config_dict)
    config_dict["inputs"][0]["slaos_key"] = "prefix1"
    config_dict["inputs"].append(deepcopy(config_dict["inputs"][0]))
    config_dict["inputs"][1]["slaos_key"] = "prefix1"
    config_dict["inputs"][1]["offset"]["type"] = "redis"
    config_dict["inputs"][1]["offset"]["start_from"] = 987654321
    config_dict["inputs"][1]["offset"]["redis"] = {
        "host": "redis",
        "port": 6379,
        "db": 0,
    }
    config_dict["inputs"].append(deepcopy(config_dict["inputs"][0]))
    config_dict["inputs"][2]["offset"]["type"] = "slaos"
    config_dict["inputs"][2]["offset"]["start_from"] = 1719788400000
    config_dict["inputs"][2]["offset"]["slaos"] = {
        "ingestion_id": "some-uuid",
        "ingestion_key": "secret-key",
        "ingestion_url": "http://localhost:8000/v1/ingest",
        "datastream_key": "datastream-key",
    }
    config = RatedIndexerYamlConfig(**config_dict)
    mock_load_config.return_value = config

    tracker1, start_from1 = get_offset_tracker("prefix1", 0)
    tracker2, start_from2 = get_offset_tracker("prefix1", 1)
    with patch.object(
        RatedAPIOffsetTracker,
        "get_offset_from_api",
        return_value=datetime(2024, 10, 1, tzinfo=UTC),
    ):
        tracker3, start_from3 = get_offset_tracker("prefix1", 2)

    assert isinstance(tracker1, PostgresOffsetTracker)
    assert tracker1.slaos_key == "prefix1_0"
    assert start_from1 == 123456789

    assert isinstance(tracker2, RedisOffsetTracker)
    assert tracker2.slaos_key == "prefix1_1"
    assert start_from2 == 987654321

    assert isinstance(tracker3, RatedAPIOffsetTracker)
    assert tracker3.slaos_key == "prefix1_2"
    assert start_from3 == 1719788400000


@patch.object(ConfigurationManager, "load_config")
def test_get_offset_tracker_prefix_not_found(mock_load_config, valid_config_dict):
    config_dict = deepcopy(valid_config_dict)
    config_dict["inputs"][0]["slaos_key"] = "prefix1"
    config = RatedIndexerYamlConfig(**config_dict)
    mock_load_config.return_value = config

    with pytest.raises(
        ValueError, match="No configuration found for slaOS key 'non_existent'"
    ):
        get_offset_tracker("non_existent")


@patch.object(ConfigurationManager, "load_config")
def test_get_offset_tracker_multiple_calls_same_prefix(
    mock_load_config, valid_config_dict
):
    config_dict = deepcopy(valid_config_dict)
    config_dict["inputs"][0]["slaos_key"] = "prefix1"
    config_dict["inputs"].append(deepcopy(config_dict["inputs"][0]))
    config_dict["inputs"][1]["slaos_key"] = "prefix1"
    config_dict["inputs"][1]["offset"]["type"] = "redis"
    config_dict["inputs"][1]["offset"]["start_from"] = 987654321
    config_dict["inputs"][1]["offset"]["redis"] = {
        "host": "redis",
        "port": 6379,
        "db": 0,
    }
    config = RatedIndexerYamlConfig(**config_dict)
    mock_load_config.return_value = config

    results = [get_offset_tracker("prefix1", i) for i in range(2)]

    assert isinstance(results[0][0], PostgresOffsetTracker)
    assert results[0][0].slaos_key == "prefix1_0"
    assert results[0][1] == 123456789

    assert isinstance(results[1][0], RedisOffsetTracker)
    assert results[1][0].slaos_key == "prefix1_1"
    assert results[1][1] == 987654321

    with pytest.raises(ValueError):
        get_offset_tracker("prefix1", 2)


@patch.object(ConfigurationManager, "load_config")
def test_get_offset_tracker_different_prefixes(mock_load_config, valid_config_dict):
    config_dict = deepcopy(valid_config_dict)
    config_dict["inputs"][0]["slaos_key"] = "prefix1"
    config_dict["inputs"].append(deepcopy(config_dict["inputs"][0]))
    config_dict["inputs"][1]["slaos_key"] = "prefix2"
    config = RatedIndexerYamlConfig(**config_dict)
    mock_load_config.return_value = config

    tracker1, start_from1 = get_offset_tracker("prefix1")
    tracker2, start_from2 = get_offset_tracker("prefix2")

    assert isinstance(tracker1, PostgresOffsetTracker)
    assert tracker1.slaos_key == "prefix1"
    assert start_from1 == 123456789

    assert isinstance(tracker2, PostgresOffsetTracker)
    assert tracker2.slaos_key == "prefix2"
    assert start_from2 == 123456789
