from unittest.mock import patch, Mock

import pytest
from sqlalchemy import inspect, select
from datetime import datetime

from src.config.manager import ConfigurationManager
from src.config.models.offset import (
    OffsetPostgresYamlConfig,
    OffsetYamlConfig,
    StartFromTypes,
    OffsetTypes,
)
from src.indexers.offset_tracker.postgres import PostgresOffsetTracker

TEST_START_FROM = 123_456
mock_config_data = OffsetYamlConfig(
    type=OffsetTypes.POSTGRES,
    postgres=OffsetPostgresYamlConfig(
        host="db",
        port=5432,
        user="user",
        database="test_db",
        password="password",
        table_name="test_table",
    ),
    start_from=TEST_START_FROM,
    start_from_type=StartFromTypes.BIGINT,
)


@pytest.fixture(scope="module")
@patch.object(
    ConfigurationManager, "load_config", return_value=Mock(offset=mock_config_data)
)
def tracker(mock_get_config):
    return PostgresOffsetTracker(integration_prefix="test", config=mock_config_data)


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
