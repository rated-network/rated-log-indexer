from contextlib import redirect_stdout, redirect_stderr
from io import StringIO

import pytest


@pytest.fixture
def test_events():
    return [
        {"id": 1, "value": "test1"},
        {"id": 2, "value": "test2"},
        {"id": 3, "value": "test3"},
    ]


@pytest.fixture
def capture_output():
    stdout = StringIO()
    stderr = StringIO()
    with redirect_stdout(stdout), redirect_stderr(stderr):
        yield stdout, stderr
