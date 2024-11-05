from contextlib import redirect_stdout, redirect_stderr
from datetime import datetime, timezone
from io import StringIO

import pytest
from pytest_httpx import HTTPXMock

from src.config.models.output import RatedOutputConfig
from src.indexers.sinks.rated import build_http_sink
from src.indexers.filters.types import FilteredEvent
from freezegun import freeze_time


@pytest.fixture
def mocked_time():
    with freeze_time("2024-09-03 12:00:00") as frozen_time:
        yield frozen_time


@pytest.fixture
def test_events():
    logs = [
        {
            "slaos_key": "",
            "organization_id": "organization_id_one",
            "idempotency_key": "mock_log_one",
            "event_timestamp": datetime.fromtimestamp(
                1723041096000 / 1000, tz=timezone.utc
            ),
            "values": {"example_key": "example_value_one"},
        },
        {
            "slaos_key": "",
            "organization_id": "organization_id_two",
            "idempotency_key": "mock_log_two",
            "event_timestamp": datetime.fromtimestamp(
                1723041096100 / 1000, tz=timezone.utc
            ),
            "values": {"example_key": "example_value_two"},
        },
        {
            "slaos_key": "",
            "organization_id": "organization_id_three",
            "idempotency_key": "mock_log_three",
            "event_timestamp": datetime.fromtimestamp(
                1723041096200 / 1000, tz=timezone.utc
            ),
            "values": {"example_key": "example_value_three"},
        },
    ]
    return [FilteredEvent(**log) for log in logs]


@pytest.fixture
def http_sink(httpx_mock: HTTPXMock):
    endpoint = "https://your_ingestion_url.com/v1/ingest"
    httpx_mock.add_response(
        method="POST",
        url=f"{endpoint}/your_ingestion_id/your_ingestion_key",
        status_code=200,
    )
    output_config = RatedOutputConfig(
        ingestion_id="your_ingestion_id",
        ingestion_key="your_ingestion_key",
        ingestion_url=endpoint,
    )
    return build_http_sink(output_config, slaos_key="")


@pytest.fixture
def capture_output():
    stdout = StringIO()
    stderr = StringIO()
    with redirect_stdout(stdout), redirect_stderr(stderr):
        yield stdout, stderr
