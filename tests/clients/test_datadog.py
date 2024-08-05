import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from src.clients.datadog import DatadogClient, DatadogConfig


class MockDatadogConfig(DatadogConfig):
    def __init__(self):
        super().__init__(
            api_key="fake_api_key",
            app_key="fake_app_key",
            indexes=["main"],
            site="datadog.eu",
            query="*",
        )


@pytest.fixture
def mock_logs_api():
    return MagicMock()


@patch("src.clients.datadog.LogsApi", autospec=True)
def test_query_logs_initial_query_success(mock_logs_api):
    mock_response = {
        "data": [{"message": "log1"}, {"message": "log2"}],
        "meta": {"status": "done"},
    }
    mock_logs_api.return_value.list_logs.return_value = mock_response

    config = MockDatadogConfig()
    datadog_client = DatadogClient(config)

    start_time = datetime(2024, 8, 2, 11, 20, 16)
    end_time = datetime(2024, 8, 2, 11, 31, 16)

    logs = list(datadog_client.query_logs(start_time, end_time))

    assert len(logs) == 2
    assert logs == [{"message": "log1"}, {"message": "log2"}]
    mock_logs_api.return_value.list_logs.assert_called_once()


@patch("src.clients.datadog.LogsApi", autospec=True)
def test_query_logs_pagination(mock_logs_api):
    mock_response1 = {
        "data": [{"message": "log1"}, {"message": "log2"}],
        "meta": {"page": {"after": "cursor1"}},
    }
    mock_response2 = {
        "data": [{"message": "log3"}, {"message": "log4"}],
        "meta": {"status": "done"},
    }
    mock_logs_api.return_value.list_logs.side_effect = [
        mock_response1,
        mock_response2,
    ]

    config = MockDatadogConfig()
    datadog_client = DatadogClient(config)

    start_time = datetime(2024, 8, 2, 11, 20, 16)
    end_time = datetime(2024, 8, 2, 11, 31, 16)

    logs = list(datadog_client.query_logs(start_time, end_time))

    assert len(logs) == 4
    assert logs == [
        {"message": "log1"},
        {"message": "log2"},
        {"message": "log3"},
        {"message": "log4"},
    ]
    assert mock_logs_api.return_value.list_logs.call_count == 2


@patch("src.clients.datadog.LogsApi", autospec=True)
def test_query_logs_handles_exceptions(mock_logs_api):
    mock_logs_api.return_value.list_logs.side_effect = Exception("Test exception")

    config = MockDatadogConfig()
    datadog_client = DatadogClient(config)

    start_time = datetime(2024, 8, 2, 11, 20, 16)
    end_time = datetime(2024, 8, 2, 11, 31, 16)

    with pytest.raises(Exception, match="Test exception"):
        list(datadog_client.query_logs(start_time, end_time))

    assert mock_logs_api.return_value.list_logs.call_count == 1


@patch("src.clients.datadog.LogsApi", autospec=True)
def test_query_logs_empty_response(mock_logs_api):
    mock_response = {"data": [], "meta": {"status": "done"}}
    mock_logs_api.return_value.list_logs.return_value = mock_response

    config = MockDatadogConfig()
    datadog_client = DatadogClient(config)

    start_time = datetime(2024, 8, 2, 11, 20, 16)
    end_time = datetime(2024, 8, 2, 11, 31, 16)

    logs = list(datadog_client.query_logs(start_time, end_time))

    assert len(logs) == 0
    mock_logs_api.return_value.list_logs.assert_called_once()
