import pytest
from unittest.mock import MagicMock, patch
from pydantic import PositiveInt

from src.clients.cloudwatch import CloudwatchClient
from src.config.models.input import CloudwatchConfig


class MockConfig(CloudwatchConfig):
    def __init__(self):
        super().__init__(
            region="us-west-2",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
            log_group_name="test-log-group",
            filter_pattern="{ $.level = 'error' }",
        )


@pytest.fixture
def mock_client():
    return MagicMock()


@patch("src.clients.cloudwatch.client")
def test_query_logs_initial_query_success(mock_client):
    mock_response = {
        "events": [{"message": "log1"}, {"message": "log2"}],
        "nextToken": "next-token",
    }
    mock_client.return_value.filter_log_events.return_value = mock_response

    config = MockConfig()
    cloudwatch_client = CloudwatchClient(config)

    start_time = PositiveInt(1625097600000)  # example start time in milliseconds
    end_time = PositiveInt(1625184000000)  # example end time in milliseconds

    logs = list(cloudwatch_client.query_logs(start_time, end_time))

    assert len(logs) == 2
    assert logs == [{"message": "log1"}, {"message": "log2"}]
    mock_client.return_value.filter_log_events.assert_called_once_with(
        logGroupName=config.log_group_name,
        startTime=start_time,
        endTime=end_time,
        filterPattern=config.filter_pattern,
        limit=cloudwatch_client.limit,
    )


@patch("src.clients.cloudwatch.client")
def test_query_logs_pagination(mock_client):
    mock_response1 = {
        "events": [{"message": "log1"}, {"message": "log2"}],
        "nextToken": "next-token",
    }
    mock_response2 = {
        "events": [{"message": "log3"}, {"message": "log4"}],
        "nextToken": None,
    }
    mock_client.return_value.filter_log_events.side_effect = [
        mock_response1,
        mock_response2,
    ]

    config = MockConfig()
    cloudwatch_client = CloudwatchClient(config, limit=2)

    start_time = PositiveInt(1625097600000)  # example start time in milliseconds
    end_time = PositiveInt(1625184000000)  # example end time in milliseconds

    logs = list(cloudwatch_client.query_logs(start_time, end_time))

    assert len(logs) == 4
    assert logs == [
        {"message": "log1"},
        {"message": "log2"},
        {"message": "log3"},
        {"message": "log4"},
    ]
    assert mock_client.return_value.filter_log_events.call_count == 2
    mock_client.return_value.filter_log_events.assert_any_call(
        logGroupName=config.log_group_name,
        startTime=start_time,
        endTime=end_time,
        filterPattern=config.filter_pattern,
        limit=cloudwatch_client.limit,
    )
    mock_client.return_value.filter_log_events.assert_any_call(
        logGroupName=config.log_group_name,
        startTime=start_time,
        endTime=end_time,
        filterPattern=config.filter_pattern,
        limit=cloudwatch_client.limit,
        nextToken="next-token",
    )


@patch("src.clients.cloudwatch.client", autospec=True)
def test_query_logs_handles_exceptions(mock_client):
    mock_client.return_value.filter_log_events.side_effect = Exception("Test exception")

    config = MockConfig()
    cloudwatch_client = CloudwatchClient(config)

    start_time = PositiveInt(1625097600000)  # example start time in milliseconds
    end_time = PositiveInt(1625184000000)  # example end time in milliseconds

    with pytest.raises(Exception, match="Test exception"):
        list(cloudwatch_client.query_logs(start_time, end_time))
