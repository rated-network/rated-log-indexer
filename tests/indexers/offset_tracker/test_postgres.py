from unittest.mock import patch, Mock

import pytest
from sqlalchemy import inspect, select
from datetime import datetime
from testcontainers.postgres import PostgresContainer  # type: ignore

from src.config.manager import ConfigurationManager
from src.config.models.offset import (
    OffsetPostgresYamlConfig,
    OffsetYamlConfig,
    StartFromTypes,
    OffsetTypes,
)
from src.indexers.offset_tracker.postgres import PostgresOffsetTracker

TEST_START_FROM = 123_456


@pytest.fixture(scope="module")
def mock_config_data(postgres_container: PostgresContainer):
    data = OffsetYamlConfig(
        type=OffsetTypes.POSTGRES,
        postgres=OffsetPostgresYamlConfig(
            host=postgres_container.get_container_host_ip(),
            port=int(postgres_container.get_exposed_port(5432)),
            user=postgres_container.username,
            database=postgres_container.dbname,
            password=postgres_container.password,
            table_name="test_table",
        ),
        start_from=TEST_START_FROM,
        start_from_type=StartFromTypes.BIGINT,
    )
    return data


@pytest.fixture(scope="module")
def mock_load_config(mock_config_data):
    with patch.object(
        ConfigurationManager, "load_config", return_value=Mock(offset=mock_config_data)
    ) as mocked_load_config:
        yield mocked_load_config


@pytest.fixture(scope="module")
def tracker(mock_config_data, mock_load_config):
    return PostgresOffsetTracker(slaos_key="test", config=mock_config_data)


def test_table_exists(tracker):
    inspector = inspect(tracker.client.engine)
    assert tracker.table_name in inspector.get_table_names()


def test_initial_data_exists(tracker):
    with tracker.client.engine.connect() as connection:
        stmt = select(tracker.table)
        result = connection.execute(stmt)
        row = result.fetchone()
        assert row is not None
        assert row.current_offset == TEST_START_FROM


def test_postgres_offset_tracker_get_current_offset(tracker):
    assert tracker.get_current_offset() == TEST_START_FROM


def test_postgres_offset_tracker_update_offset(tracker):
    tracker.get_current_offset()
    new_offset = TEST_START_FROM + 100
    tracker.update_offset(new_offset)
    assert tracker.get_current_offset() == new_offset


def test_postgres_offset_tracker_with_datetime(tracker):
    tracker.config.start_from_type = "datetime"
    new_offset = int(datetime.now().timestamp() * 1000)
    tracker.update_offset(new_offset)
    assert tracker.get_current_offset() == new_offset
