from copy import deepcopy
from datetime import UTC, datetime

import pytest
from unittest.mock import patch
from src.config.manager import RatedIndexerYamlConfig
from src.indexers.offset_tracker.factory import get_offset_tracker
from src.indexers.offset_tracker.postgres import PostgresOffsetTracker
from src.indexers.offset_tracker.rated import RatedAPIOffsetTracker
from src.indexers.offset_tracker.redis import RedisOffsetTracker


def test_get_offset_tracker_no_duplicates(valid_config_dict):
    config_dict = deepcopy(valid_config_dict)
    config_dict["inputs"][0]["slaos_key"] = "prefix1"
    config = RatedIndexerYamlConfig(**config_dict)

    tracker, start_from = get_offset_tracker("prefix1", config=config)

    assert isinstance(tracker, PostgresOffsetTracker)
    assert tracker.slaos_key == "prefix1"
    assert start_from == 123456789


def test_get_offset_tracker_with_duplicates(valid_config_dict):
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
        "datastream_filter": {
            "key": "datastream-key",
        },
    }
    config = RatedIndexerYamlConfig(**config_dict)

    tracker1, start_from1 = get_offset_tracker("prefix1", 0, config=config)
    tracker2, start_from2 = get_offset_tracker("prefix1", 1, config=config)
    with patch.object(
        RatedAPIOffsetTracker,
        "get_offset_from_api",
        return_value=datetime(2024, 10, 1, tzinfo=UTC),
    ):
        tracker3, start_from3 = get_offset_tracker("prefix1", 2, config=config)

    assert isinstance(tracker1, PostgresOffsetTracker)
    assert tracker1.slaos_key == "prefix1_0"
    assert start_from1 == 123456789

    assert isinstance(tracker2, RedisOffsetTracker)
    assert tracker2.slaos_key == "prefix1_1"
    assert start_from2 == 987654321

    assert isinstance(tracker3, RatedAPIOffsetTracker)
    assert tracker3.slaos_key == "prefix1_2"
    assert start_from3 == 1719788400000


def test_get_offset_tracker_prefix_not_found(valid_config_dict):
    config_dict = deepcopy(valid_config_dict)
    config_dict["inputs"][0]["slaos_key"] = "prefix1"
    config = RatedIndexerYamlConfig(**config_dict)

    with pytest.raises(
        ValueError, match="No configuration found for slaOS key 'non_existent'"
    ):
        get_offset_tracker("non_existent", config=config)


def test_get_offset_tracker_multiple_calls_same_prefix(valid_config_dict):
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

    results = [get_offset_tracker("prefix1", i, config=config) for i in range(2)]

    assert isinstance(results[0][0], PostgresOffsetTracker)
    assert results[0][0].slaos_key == "prefix1_0"
    assert results[0][1] == 123456789

    assert isinstance(results[1][0], RedisOffsetTracker)
    assert results[1][0].slaos_key == "prefix1_1"
    assert results[1][1] == 987654321

    with pytest.raises(ValueError):
        get_offset_tracker("prefix1", 2, config=config)


def test_get_offset_tracker_different_prefixes(valid_config_dict):
    config_dict = deepcopy(valid_config_dict)
    config_dict["inputs"][0]["slaos_key"] = "prefix1"
    config_dict["inputs"].append(deepcopy(config_dict["inputs"][0]))
    config_dict["inputs"][1]["slaos_key"] = "prefix2"
    config = RatedIndexerYamlConfig(**config_dict)

    tracker1, start_from1 = get_offset_tracker("prefix1", config=config)
    tracker2, start_from2 = get_offset_tracker("prefix2", config=config)

    assert isinstance(tracker1, PostgresOffsetTracker)
    assert tracker1.slaos_key == "prefix1"
    assert start_from1 == 123456789

    assert isinstance(tracker2, PostgresOffsetTracker)
    assert tracker2.slaos_key == "prefix2"
    assert start_from2 == 123456789


def test_slaos_config_with_customer_id(valid_config_dict):
    config_dict = deepcopy(valid_config_dict)
    config_dict["inputs"][0]["offset"] = {
        "type": "slaos",
        "start_from": 123456789,
        "start_from_type": "bigint",
        "slaos": {
            "ingestion_id": "ingestion_id",
            "ingestion_key": "ingestion_key",
            "ingestion_url": "https://your_ingestion_url.com/v1/ingest",
            "datastream_filter": {
                "key": "datastream-key",
                "organization_id": "hash:customer_id",
            },
        },
    }
    config = RatedIndexerYamlConfig(**config_dict)

    assert config.inputs[0].offset.type == "slaos"
    assert len(config.inputs[0].offset.slaos.datastream_filter.organization_id) == 64
