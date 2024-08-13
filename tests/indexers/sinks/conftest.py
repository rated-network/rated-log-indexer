from contextlib import redirect_stdout, redirect_stderr
from datetime import datetime, timezone
from io import StringIO

import pytest

from src.indexers.filters.types import FilteredEvent


@pytest.fixture
def test_events():
    logs = [
        {
            "customer_id": "customer_id_one",
            "log_id": "mock_log_one",
            "event_timestamp": datetime.fromtimestamp(
                1723041096000 / 1000, tz=timezone.utc
            ),
            "values": '{"example_key": "example_value_one"}',
        },
        {
            "customer_id": "customer_id_two",
            "log_id": "mock_log_two",
            "event_timestamp": datetime.fromtimestamp(
                1723041096100 / 1000, tz=timezone.utc
            ),
            "values": '{"example_key": "example_value_two"}',
        },
        {
            "customer_id": "customer_id_three",
            "log_id": "mock_log_three",
            "event_timestamp": datetime.fromtimestamp(
                1723041096200 / 1000, tz=timezone.utc
            ),
            "values": '{"example_key": "example_value_three"}',
        },
    ]
    return [FilteredEvent(**log) for log in logs]


@pytest.fixture
def capture_output():
    stdout = StringIO()
    stderr = StringIO()
    with redirect_stdout(stdout), redirect_stderr(stderr):
        yield stdout, stderr
