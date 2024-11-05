import pytest
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError  # type: ignore
from pydantic import PositiveInt

from src.clients.cloudwatch import (
    CloudwatchClient,
    CloudwatchSupportedInputTypes,
    CloudwatchClientError,
)
from src.config.models.inputs.cloudwatch import (
    CloudwatchConfig,
    CloudwatchLogsConfig,
    CloudwatchMetricsConfig,
)


class MockConfig(CloudwatchConfig):
    def __init__(self):
        super().__init__(
            region="us-west-2",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
            logs_config=CloudwatchLogsConfig(
                log_group_name="test-log-group",
                filter_pattern="{ $.level = 'error' }",
            ),
            metrics_config=CloudwatchMetricsConfig(
                namespace="test-namespace",
                metric_name="test_metric_label",
                period=60,
                statistic="AVERAGE",
                organization_identifier="organization_id",
                metric_queries=[  # type: ignore
                    [{"name": "organization_id", "value": "customer1"}],
                ],
            ),
        )


@pytest.fixture
def mock_client():
    return MagicMock()


@patch("src.clients.cloudwatch.client")
def test_query_logs_initial_query_success(mock_client):
    # Mock responses for two pages of results
    mock_responses = [
        {
            "events": [{"message": "log1"}, {"message": "log2"}],
            "nextToken": "next-token",
        },
        {
            "events": [],
            "nextToken": None,  # No more pages after this
        },
    ]

    mock_client.return_value.filter_log_events = MagicMock(side_effect=mock_responses)

    config = MockConfig()
    cloudwatch_client = CloudwatchClient(config)

    start_time = PositiveInt(1625097600000)  # example start time in milliseconds
    end_time = PositiveInt(1625184000000)  # example end time in milliseconds

    logs = list(cloudwatch_client.query_logs(start_time, end_time))

    # Assert that we got all logs from the first page
    assert len(logs) == 2
    assert logs == [{"message": "log1"}, {"message": "log2"}]
    assert mock_client.return_value.filter_log_events.call_count == 2

    # Check the parameters of the first call
    mock_client.return_value.filter_log_events.assert_any_call(
        logGroupName=config.logs_config.log_group_name,
        startTime=start_time,
        endTime=end_time,
        filterPattern=config.logs_config.filter_pattern,
        limit=cloudwatch_client.logs_query_limit,
    )

    # Check the parameters of the second call (should include the nextToken)
    mock_client.return_value.filter_log_events.assert_any_call(
        logGroupName=config.logs_config.log_group_name,
        startTime=start_time,
        endTime=end_time,
        filterPattern=config.logs_config.filter_pattern,
        limit=cloudwatch_client.logs_query_limit,
        nextToken="next-token",
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
        logGroupName=config.logs_config.log_group_name,
        startTime=start_time,
        endTime=end_time,
        filterPattern=config.logs_config.filter_pattern,
        limit=cloudwatch_client.logs_query_limit,
    )
    mock_client.return_value.filter_log_events.assert_any_call(
        logGroupName=config.logs_config.log_group_name,
        startTime=start_time,
        endTime=end_time,
        filterPattern=config.logs_config.filter_pattern,
        limit=cloudwatch_client.logs_query_limit,
        nextToken="next-token",
    )


@patch("src.clients.cloudwatch.client", autospec=True)
def test_query_logs_handles_exceptions(mock_client):
    mock_client.return_value.filter_log_events.side_effect = Exception("Test exception")

    config = MockConfig()
    cloudwatch_client = CloudwatchClient(config)

    start_time = PositiveInt(1625097600000)  # example start time in milliseconds
    end_time = PositiveInt(1625184000000)  # example end time in milliseconds

    with pytest.raises(Exception, match="Failed to query logs for test-log-group"):
        list(cloudwatch_client.query_logs(start_time, end_time))


@patch("src.clients.cloudwatch.client")
def test_query_metrics(mock_client):
    # Mock responses
    mock_response1 = {
        "MetricDataResults": [
            {
                "Id": "test_metric_label_query_0",
                "Timestamps": [1625097600000, 1625097660000],
                "Values": [1.0, 2.0],
            },
            {
                "Id": "test_metric_label_query_0",
                "Timestamps": [1625097720000],
                "Values": [3.0],
            },
        ]
    }

    mock_client.return_value.get_metric_data.side_effect = [
        mock_response1,
    ]

    config = MockConfig()
    cloudwatch_client = CloudwatchClient(config, limit=5)

    start_time = PositiveInt(1625097600000)  # example start time in milliseconds
    end_time = PositiveInt(1625184000000)  # example end time in milliseconds

    # Collect metrics
    metrics = list(cloudwatch_client.query_metrics(start_time, end_time))

    expected_metrics = [
        {
            "organization_id": "customer1",
            "timestamp": 1625097600000,
            "value": 1.0,
            "label": "test_metric_label",
        },
        {
            "organization_id": "customer1",
            "timestamp": 1625097660000,
            "value": 2.0,
            "label": "test_metric_label",
        },
        {
            "organization_id": "customer1",
            "timestamp": 1625097720000,
            "value": 3.0,
            "label": "test_metric_label",
        },
    ]

    # Assertions
    assert len(metrics) == len(expected_metrics)
    assert metrics == expected_metrics

    assert mock_client.return_value.get_metric_data.call_count == 1


def test_parse_metrics_queries():
    config = MockConfig()
    cloudwatch_client = CloudwatchClient(config)
    organization_id_map, query_chunks = cloudwatch_client._parse_metrics_queries(
        config.metrics_config
    )

    expected_organization_id_map = {"test_metric_label_query_0": "customer1"}
    expected_query_chunks = [
        [
            {
                "Id": "test_metric_label_query_0",
                "MetricStat": {
                    "Metric": {
                        "Namespace": "test-namespace",
                        "MetricName": "test_metric_label",
                        "Dimensions": [
                            {"Name": "organization_id", "Value": "customer1"}
                        ],
                    },
                    "Period": 60,
                    "Stat": "Average",
                },
                "ReturnData": True,
            }
        ]
    ]

    assert organization_id_map == expected_organization_id_map
    assert query_chunks == expected_query_chunks


@patch("src.clients.cloudwatch.client")
def test_retry_on_error(mock_client):
    client = CloudwatchClient(MockConfig())

    client.logs_client.filter_log_events.side_effect = ClientError(
        {"Error": {"Code": "ThrottlingException"}}, "filter_log_events"
    )

    params = {
        "logGroupName": "test-group",
        "startTime": 1625097600000,
        "endTime": 1625184000000,
        "limit": 10,
    }

    with pytest.raises(
        CloudwatchClientError,
        match="Rate limit hit, retrying",
    ):
        client.make_api_call(CloudwatchSupportedInputTypes.LOGS, params)

    assert client.logs_client.filter_log_events.call_count == 10
