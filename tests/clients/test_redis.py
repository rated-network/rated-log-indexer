import pytest
from testcontainers.redis import RedisContainer  # type: ignore

from src.clients.redis import RedisConfig, RedisClient


@pytest.fixture(scope="function")
def redis_client():
    config = RedisConfig(host="redis", port=6379, db=0)
    client = RedisClient(config)
    yield client
    client.close()


def test_redis_client_init(redis_client):
    assert redis_client.client is not None


def test_redis_client_close(redis_client):
    redis_client.close()
    assert redis_client.client is None


def test_redis_client_get_set(redis_container: RedisContainer):
    config = RedisConfig(
        host=redis_container.get_container_host_ip(),
        port=redis_container.get_exposed_port(6379),
        db=0,
    )
    client = RedisClient(config)
    client.set("test_key", "test_value")
    value = client.get("test_key")
    assert value == "test_value"
