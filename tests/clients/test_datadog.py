import time

import pytest
from unittest.mock import MagicMock, patch

from datadog_api_client.exceptions import ApiException
from datadog_api_client.v2.model.timeseries_response_values import (
    TimeseriesResponseValues,
)
from pydantic import PositiveInt

from src.config.models.inputs.datadog import (
    DatadogLogsConfig,
    DatadogMetricsConfig,
)
from src.clients.datadog import DatadogClient, DatadogConfig, DatadogClientError


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
                metric_tag_data=[  # type: ignore
                    {"customer_value": "customer1", "tag_string": "customer:customer1"},
                    {"customer_value": "customer2", "tag_string": "customer:customer2"},
                ],
            ),
        )


class MockResponse:
    def __init__(self, data):
        self.data = data

    def to_dict(self):
        return self.data


@pytest.fixture
def mock_logs_api():
    return MagicMock()


@pytest.fixture(autouse=True)
def mock_sleep(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda x: None)


@patch("src.clients.datadog.LogsApi", autospec=True)
def test_query_logs_initial_query_success(mock_logs_api):

    mock_response = {
        "data": [MockResponse({"message": "log1"}), MockResponse({"message": "log2"})],
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
        "data": [MockResponse({"message": "log1"}), MockResponse({"message": "log2"})],
        "meta": {"page": {"after": "cursor1"}},
    }
    mock_response2 = {
        "data": [MockResponse({"message": "log3"}), MockResponse({"message": "log4"})],
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
    mock_logs_api.return_value.list_logs.side_effect = Exception()

    config = MockDatadogConfig()
    datadog_client = DatadogClient(config)

    start_time = 1722597616000  # 2024-08-02T11:20:16
    end_time = 1722598276000  # 2024-08-02T11:31:16

    with pytest.raises(DatadogClientError, match="Failed to query logs"):
        list(datadog_client.query_logs(start_time, end_time))

    assert mock_logs_api.return_value.list_logs.call_count == 10


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
                "values": [
                    TimeseriesResponseValues([1.0, 2.0, 3.0]),
                    TimeseriesResponseValues([4.0, 5.0, 6.0]),
                ],
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
                    TimeseriesResponseValues(
                        [1.0, 2.0, None]
                    ),  # Corresponds to query_index 0
                    TimeseriesResponseValues(
                        [None, None, 3.0]
                    ),  # Corresponds to query_index 1
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


def test_retry_on_429_error():
    mock_config = MockDatadogConfig()
    datadog_client = DatadogClient(mock_config)

    # Create a real ApiException
    error_429 = ApiException(status=429, reason="Too Many Requests")
    error_429.headers = {"x-ratelimit-reset": "3"}

    mock_response = MockResponse(
        data={
            "attributes": {
                "series": [{"query_index": 0}, {"query_index": 1}],
                "times": [1625097600000, 1625097660000, 1625097720000],
                "values": [
                    TimeseriesResponseValues(
                        [1.0, 2.0, None]
                    ),  # Corresponds to query_index 0
                    TimeseriesResponseValues(
                        [None, None, 3.0]
                    ),  # Corresponds to query_index 1
                ],
            }
        }
    )

    # Mock the metrics_api.query_timeseries_data method
    with patch.object(
        datadog_client.metrics_api,
        "query_timeseries_data",
        side_effect=[error_429, error_429, mock_response],
    ) as mock_query:
        # Call query_metrics and exhaust the iterator
        list(datadog_client.query_metrics(1000, 2000))

    # Check that the API was called the expected number of times (including retries)
    assert mock_query.call_count == 3

    # Test with continuous failures to ensure DatadogClientError is raised
    with patch.object(
        datadog_client.metrics_api, "query_timeseries_data", side_effect=error_429
    ):
        with pytest.raises(DatadogClientError) as excinfo:
            list(datadog_client.query_metrics(1000, 2000))

    assert "Failed to query Datadog metrics" in str(excinfo.value)
