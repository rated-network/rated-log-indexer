import pytest
from sqlalchemy.orm import Session
from src.clients.postgres import PostgresConfig, PostgresClient


def valid_config():
    config = PostgresConfig(
        host="localhost",
        port=5432,
        user="user",
        database="test_db",
        password="password",
        dsn="",
    )
    return config


def invalid_config():
    return {
        "host": "localhost",
        "port": "invalid_port",
        "user": "user",
        "database": "test_db",
        "password": "password",
    }


def test_creates_valid_dsn():
    config = valid_config()
    assert config.dsn == "postgresql://user:password@localhost:5432/test_db"


def test_raises_error_on_invalid_config():
    with pytest.raises(TypeError):
        PostgresConfig(**invalid_config())


def test_creates_session_successfully():
    config = valid_config()
    client = PostgresClient(config)
    assert isinstance(client.session, Session)
    client.close()


def test_closes_session_successfully():
    config = valid_config()
    client = PostgresClient(config)
    client.close()
    assert client.session is None
