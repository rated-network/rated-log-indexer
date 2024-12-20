from datetime import UTC, datetime
from hashlib import sha256

import pytest
import unittest.mock

from src.config.models.offset import (
    OffsetSlaosYamlConfig,
    OffsetTypes,
    OffsetYamlConfig,
    StartFromTypes,
    OffsetSlaosYamlFilter,
)
from src.indexers.offset_tracker import rated


INGESTION_ID = "some-uuid"
INGESTION_KEY = "secret-key"
INGESTION_URL = "http://localhost:8000/v1/ingest"
DATASTREAM_KEY = "datastream-key"
CUSTOMER_ID = "hash:test-customer-123"
HASHED_CUSTOMER_ID = sha256(CUSTOMER_ID[5:].encode()).hexdigest()


@pytest.fixture
def yaml_config():
    slaos_config = OffsetSlaosYamlConfig(
        ingestion_id=INGESTION_ID,
        ingestion_key=INGESTION_KEY,
        ingestion_url=INGESTION_URL,
        datastream_filter=OffsetSlaosYamlFilter(key=DATASTREAM_KEY),
    )
    config = OffsetYamlConfig(
        type=OffsetTypes.SLAOS,
        override_start_from=False,
        start_from=1234,
        start_from_type=StartFromTypes.BIGINT,
        slaos=slaos_config,
    )
    return config


@pytest.fixture
def yaml_config_with_customer():
    slaos_config = OffsetSlaosYamlConfig(
        ingestion_id=INGESTION_ID,
        ingestion_key=INGESTION_KEY,
        ingestion_url=INGESTION_URL,
        datastream_filter=OffsetSlaosYamlFilter(
            key=DATASTREAM_KEY, organization_id=CUSTOMER_ID
        ),
    )
    config = OffsetYamlConfig(
        type=OffsetTypes.SLAOS,
        override_start_from=False,
        start_from=3456,
        start_from_type=StartFromTypes.BIGINT,
        slaos=slaos_config,
    )
    return config


def test_rated_api_offset_tracker_initialise_offset_none(yaml_config: OffsetYamlConfig):
    with unittest.mock.patch.object(
        rated.RatedAPIOffsetTracker, "get_offset_from_api", return_value=None
    ):
        tracker = rated.RatedAPIOffsetTracker(yaml_config, "foo")
        assert tracker.get_current_offset() == 1234


def test_rated_api_offset_tracker_initialise_offset_none_with_customer(
    yaml_config_with_customer: OffsetYamlConfig,
):
    with unittest.mock.patch.object(
        rated.RatedAPIOffsetTracker, "get_offset_from_api", return_value=None
    ):
        tracker = rated.RatedAPIOffsetTracker(yaml_config_with_customer, "foo")

        assert tracker.get_current_offset() == 3456
        assert tracker.config.slaos.datastream_filter.organization_id == HASHED_CUSTOMER_ID  # type: ignore[union-attr]


def test_rated_api_offset_tracker_initialise_offset_some(yaml_config: OffsetYamlConfig):
    timestamp = datetime(2024, 5, 31, 16, 8, 37, 171828, tzinfo=UTC)
    with unittest.mock.patch.object(
        rated.RatedAPIOffsetTracker, "get_offset_from_api", return_value=timestamp
    ):
        tracker = rated.RatedAPIOffsetTracker(yaml_config, "foo")
        assert tracker.get_current_offset() == 1717171717171


def test_rated_api_offset_tracker_initialise_offset_override(
    yaml_config: OffsetYamlConfig,
):
    yaml_config.override_start_from = True
    with unittest.mock.patch.object(
        rated.RatedAPIOffsetTracker, "get_offset_from_api", return_value=None
    ) as mocked_get_offset:
        tracker = rated.RatedAPIOffsetTracker(yaml_config, "foo")
        assert tracker.get_current_offset() == 1234
        mocked_get_offset.assert_not_called()


def test_rated_api_offset_tracker_update_offset(
    yaml_config: OffsetYamlConfig,
):
    timestamp = datetime(2024, 5, 31, 16, 8, 37, 171828, tzinfo=UTC)
    with unittest.mock.patch.object(
        rated.RatedAPIOffsetTracker, "get_offset_from_api", return_value=timestamp
    ):
        tracker = rated.RatedAPIOffsetTracker(yaml_config, "foo")
        assert tracker.get_current_offset() == 1717171717171

    new_offset = 17171717142
    tracker.update_offset(new_offset)

    assert tracker.get_current_offset() == new_offset
