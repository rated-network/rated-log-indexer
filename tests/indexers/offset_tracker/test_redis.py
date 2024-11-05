import pytest
from testcontainers.redis import RedisContainer  # type: ignore

from src.config.models.offset import (
    OffsetRedisYamlConfig,
    OffsetYamlConfig,
    StartFromTypes,
    OffsetTypes,
)
from src.indexers.offset_tracker.redis import RedisOffsetTracker
from src.clients.redis import RedisClient, RedisConfig

TEST_START_FROM = 123_456


@pytest.fixture(scope="module")
def mock_config_data(redis_container: RedisContainer):
    data = OffsetYamlConfig(
        type=OffsetTypes.REDIS,
        redis=OffsetRedisYamlConfig(
            host=redis_container.get_container_host_ip(),
            port=int(redis_container.get_exposed_port(6379)),
            db=0,
            key="test_key",
        ),
        start_from=TEST_START_FROM,
        start_from_type=StartFromTypes.BIGINT,
    )
    return data


@pytest.fixture(scope="module")
def redis_client(mock_config_data):
    redis_config = RedisConfig(
        host=mock_config_data.redis.host,
        port=mock_config_data.redis.port,
        db=mock_config_data.redis.db,
    )
    client = RedisClient(redis_config)
    yield client
    # Clean up after tests
    client.set("test:test_key", "")  # Set to empty string instead of None
    client.close()


@pytest.fixture
def tracker(mock_config_data, redis_client):
    return RedisOffsetTracker(slaos_key="test", config=mock_config_data)


def test_redis_offset_tracker_init(tracker, mock_config_data):
    assert tracker.key == "test:test_key"
    assert tracker.config == mock_config_data


def test_redis_offset_tracker_get_current_offset_initial(tracker):
    assert tracker.get_current_offset() == TEST_START_FROM


def test_redis_offset_tracker_get_current_offset_existing(tracker, redis_client):
    existing_offset = 654321
    redis_client.set(tracker.key, str(existing_offset))
    assert tracker.get_current_offset() == existing_offset


def test_redis_offset_tracker_update_offset(tracker, redis_client):
    new_offset = TEST_START_FROM + 100
    tracker.update_offset(new_offset)
    assert int(redis_client.get(tracker.key)) == new_offset


def test_redis_offset_tracker_override_start_from(tracker, redis_client):
    redis_client.set(tracker.key, "")  # Set to empty string to simulate deletion
    tracker.config.override_start_from = True
    assert tracker.get_current_offset() == TEST_START_FROM
    assert int(redis_client.get(tracker.key)) == TEST_START_FROM


def test_redis_offset_tracker_invalid_type(mock_config_data):
    invalid_config = mock_config_data.model_copy()
    invalid_config.type = OffsetTypes.POSTGRES
    with pytest.raises(
        ValueError,
        match="Offset tracker type is not set to 'redis' in the configuration",
    ):
        RedisOffsetTracker(slaos_key="test", config=invalid_config)
