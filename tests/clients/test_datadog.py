import pytest
from unittest.mock import MagicMock, patch

from pydantic import PositiveInt

from src.config.models.inputs.datadog import (
    DatadogLogsConfig,
    DatadogTag,
    DatadogMetricsConfig,
)
from src.clients.datadog import DatadogClient, DatadogConfig


class MockDatadogConfig(DatadogConfig):
    def __init__(self):
        super().__init__(
            api_key="fake_api_key",
            app_key="fake_app_key",
            site="datadog.eu",
            logs_config=DatadogLogsConfig(
                indexes=["main"],
                query="*",
            ),
            metrics_config=DatadogMetricsConfig(
                metric_name="test.metric",
                interval=60,
                statistic="AVERAGE",
                customer_identifier="customer",
                metric_tag_data=[
                    DatadogTag(
                        customer_value="customer1", tag_string="customer:customer1"
                    ),
                    DatadogTag(
                        customer_value="customer2", tag_string="customer:customer2"
                    ),
                ],
            ),
        )


class MockLogEntry:
    def __init__(self, data):
        self.data = data

    def to_dict(self):
        return self.data


@pytest.fixture
def mock_logs_api():
    return MagicMock()


@patch("src.clients.datadog.LogsApi", autospec=True)
def test_query_logs_initial_query_success(mock_logs_api):
    mock_response = {
        "data": [MockLogEntry({"message": "log1"}), MockLogEntry({"message": "log2"})],
        "meta": {"status": "done"},
    }
    mock_logs_api.return_value.list_logs.return_value = mock_response

    config = MockDatadogConfig()
    datadog_client = DatadogClient(config)

    start_time = 1722597616000  # 2024-08-02T11:20:16
    end_time = 1722598276000  # 2024-08-02T11:31:16

    logs = list(datadog_client.query_logs(start_time, end_time))

    assert len(logs) == 2
    assert logs == [{"message": "log1"}, {"message": "log2"}]
    mock_logs_api.return_value.list_logs.assert_called_once()


@patch("src.clients.datadog.LogsApi", autospec=True)
def test_query_logs_pagination(mock_logs_api):
    mock_response1 = {
        "data": [MockLogEntry({"message": "log1"}), MockLogEntry({"message": "log2"})],
        "meta": {"page": {"after": "cursor1"}},
    }
    mock_response2 = {
        "data": [MockLogEntry({"message": "log3"}), MockLogEntry({"message": "log4"})],
        "meta": {"status": "done"},
    }
    mock_logs_api.return_value.list_logs.side_effect = [
        mock_response1,
        mock_response2,
    ]

    config = MockDatadogConfig()
    datadog_client = DatadogClient(config)

    start_time = 1722597616000  # 2024-08-02T11:20:16
    end_time = 1722598276000  # 2024-08-02T11:31:16

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

    start_time = 1722597616000  # 2024-08-02T11:20:16
    end_time = 1722598276000  # 2024-08-02T11:31:16

    with pytest.raises(Exception, match="Test exception"):
        list(datadog_client.query_logs(start_time, end_time))

    assert mock_logs_api.return_value.list_logs.call_count == 1


@patch("src.clients.datadog.LogsApi", autospec=True)
def test_query_logs_empty_response(mock_logs_api):
    mock_response = {"data": [], "meta": {"status": "done"}}
    mock_logs_api.return_value.list_logs.return_value = mock_response

    config = MockDatadogConfig()
    datadog_client = DatadogClient(config)

    start_time = 1722597616000  # 2024-08-02T11:20:16
    end_time = 1722598276000  # 2024-08-02T11:31:16

    logs = list(datadog_client.query_logs(start_time, end_time))

    assert len(logs) == 0
    mock_logs_api.return_value.list_logs.assert_called_once()


@patch("src.clients.datadog.MetricsApi")  # Mock the Datadog API client
def test_query_metrics(mock_metrics_api):
    # Mock response data from the Datadog API
    mock_response = {
        "data": {
            "attributes": {
                "series": [
                    {
                        "query_index": 0,
                    },
                    {
                        "query_index": 1,
                    },
                ],
                "times": [1625097600000, 1625097660000, 1625097720000],
                "values": [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]],
            }
        }
    }

    # Configure the mock to return the mock response
    mock_metrics_api.return_value.query_timeseries_data.return_value.to_dict.return_value = (
        mock_response
    )

    # Create a mock config
    mock_config = MockDatadogConfig()
    datadog_client = DatadogClient(mock_config)

    # Set example start and end times in milliseconds
    start_time = PositiveInt(1625097600000)
    end_time = PositiveInt(1625184000000)

    # Call the query_metrics method
    metrics = list(datadog_client.query_metrics(start_time, end_time))

    # Expected metrics after processing the response
    expected_metrics = [
        {
            "metric_name": "test.metric",
            "customer_id": "customer1",
            "timestamp": 1625097600000,
            "value": 1.0,
        },
        {
            "metric_name": "test.metric",
            "customer_id": "customer1",
            "timestamp": 1625097660000,
            "value": 2.0,
        },
        {
            "metric_name": "test.metric",
            "customer_id": "customer1",
            "timestamp": 1625097720000,
            "value": 3.0,
        },
        {
            "metric_name": "test.metric",
            "customer_id": "customer2",
            "timestamp": 1625097600000,
            "value": 4.0,
        },
        {
            "metric_name": "test.metric",
            "customer_id": "customer2",
            "timestamp": 1625097660000,
            "value": 5.0,
        },
        {
            "metric_name": "test.metric",
            "customer_id": "customer2",
            "timestamp": 1625097720000,
            "value": 6.0,
        },
    ]

    # Assertions
    assert len(metrics) == len(expected_metrics)
    assert metrics == expected_metrics

    # Ensure the mock API call was made once
    assert mock_metrics_api.return_value.query_timeseries_data.call_count == 1


def test_parse_metrics_response():
    # Instantiate the mock config
    mock_config = MockDatadogConfig()

    # Mock response to simulate Datadog API response
    mock_response = {
        "data": {
            "attributes": {
                "series": [{"query_index": 0}, {"query_index": 1}],
                "times": [1625097600000, 1625097660000, 1625097720000],
                "values": [
                    [1.0, 2.0, None],  # Corresponds to query_index 0
                    [None, None, 3.0],  # Corresponds to query_index 1
                ],
            }
        }
    }

    datadog_client = DatadogClient(mock_config)
    metrics = datadog_client._parse_metrics_response(mock_response)

    # Define the expected output
    expected_metrics = [
        {
            "metric_name": "test.metric",
            "customer_id": "customer1",
            "query_index": 0,
            "data": [
                {"timestamp": 1625097600000, "value": 1.0},
                {"timestamp": 1625097660000, "value": 2.0},
            ],
        },
        {
            "metric_name": "test.metric",
            "customer_id": "customer2",
            "query_index": 1,
            "data": [{"timestamp": 1625097720000, "value": 3.0}],
        },
    ]

    assert (
        metrics == expected_metrics
    ), f"Expected {expected_metrics}, but got {metrics}"
