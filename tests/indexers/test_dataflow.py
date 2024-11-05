import json
from datetime import timedelta, datetime, timezone
from unittest.mock import patch, MagicMock

import pytest
from bytewax.testing import run_main, TestingSource
from pytest_httpx import HTTPXMock
from rated_parser.payloads.inputs import JsonFieldDefinition, LogFormat, FieldType  # type: ignore
from testcontainers.redis import RedisContainer  # type: ignore

from src.config.models.offset import (
    OffsetYamlConfig,
    OffsetRedisYamlConfig,
    OffsetTypes,
    StartFromTypes,
)
from src.config.models.inputs.cloudwatch import CloudwatchConfig
from src.config.models.inputs.datadog import (
    DatadogConfig,
    DatadogMetricsConfig,
)
from src.indexers.filters.types import LogEntry, MetricEntry
from src.config.models.filters import FiltersYamlConfig
from src.indexers.filters.manager import FilterManager
from src.config.models.output import RatedOutputConfig
from src.config.models.inputs.input import IntegrationTypes, InputTypes, InputYamlConfig
from src.config.models.output import OutputTypes
from src.indexers.sinks.rated import build_http_sink
from src.indexers.sources.rated import TimeRange, FetchInterval, RatedPartition
from src.config.manager import RatedIndexerYamlConfig
from src.indexers.dataflow import build_dataflow


@pytest.fixture
def mock_prometheus_query():
    # Just return an empty iterator - we only care about timing
    return MagicMock(return_value=iter([]))


@pytest.fixture
def mock_time():
    with patch("src.indexers.sources.rated.datetime") as dt_mock:
        # Set a fixed "now" time
        fixed_time = datetime(2024, 1, 1, 10, 1, tzinfo=timezone.utc)
        dt_mock.now.return_value = fixed_time
        yield dt_mock


@pytest.fixture
def mock_offset_tracker():
    mock = MagicMock()
    mock.get_current_offset.return_value = int(
        datetime(2024, 1, 1, 9, 0, tzinfo=timezone.utc).timestamp() * 1000
    )
    return mock


@pytest.fixture
def mock_get_offset_tracker(mock_offset_tracker):
    with patch("src.indexers.sources.rated.get_offset_tracker") as mock:
        mock.return_value = (
            mock_offset_tracker,
            mock_offset_tracker.get_current_offset(),
        )
        yield mock


@pytest.fixture
def mocked_ingestion_endpoint(httpx_mock: HTTPXMock):
    endpoint = "https://your_ingestion_url.com/v1/ingest"
    httpx_mock.add_response(
        method="POST",
        url=f"{endpoint}/your_ingestion_id/your_ingestion_key",
        status_code=200,
    )
    return endpoint


@patch("src.indexers.dataflow.fetch_logs")
@patch("src.config.manager.ConfigurationManager.load_config")
def test_logs_dataflow(
    mock_load_config,
    mock_fetch_cloudwatch_logs,
    httpx_mock: HTTPXMock,
    valid_config_dict,
    mocked_ingestion_endpoint,
    redis_container: RedisContainer,
):
    valid_config = RatedIndexerYamlConfig(**valid_config_dict)
    mock_load_config.return_value = valid_config
    sample_logs = [
        {
            "eventId": "mock_log_one",
            "timestamp": 1723041096000,
            "message": '{"example_key": "example_value_one", "secret_key": 1, "data": {"organization_id": "customer_one"}}',
        },
        {
            "eventId": "mock_log_two",
            "timestamp": 1723041096100,
            "message": '{"example_key": "example_value_two", "secret_key": 2, "data": {"organization_id": "customer_two"}}',
        },
    ]
    sample_log_entries = [LogEntry.from_cloudwatch_log(log) for log in sample_logs]
    mock_fetch_cloudwatch_logs.return_value = iter(sample_log_entries)

    output_config = RatedOutputConfig(
        ingestion_id="your_ingestion_id",
        ingestion_key="your_ingestion_key",
        ingestion_url=mocked_ingestion_endpoint,
    )
    filter_config = FiltersYamlConfig(
        version=1,
        log_format=LogFormat.JSON,
        log_example={
            "message": {
                "example_key": "example_value_two",
                "organization_id": "customer_two",
            },
        },
        fields=[
            JsonFieldDefinition(
                key="example_key",
                field_type=FieldType.STRING,
                path="example_key",
            ),
            JsonFieldDefinition(
                key="organization_id",
                field_type=FieldType.STRING,
                path="data.organization_id",
            ),
            JsonFieldDefinition(
                key="secret_key",
                field_type=FieldType.INTEGER,
                path="secret_key",
                hash=True,
            ),
        ],
    )
    filter_manager = FilterManager(filter_config, "slaos_key", InputTypes.LOGS)

    mock_input = TestingSource([TimeRange(start_time=1, end_time=2)])
    inputs = [
        (
            IntegrationTypes.CLOUDWATCH,
            InputTypes.LOGS,
            valid_config.inputs[0].cloudwatch,
            mock_input,
            mock_fetch_cloudwatch_logs,
            filter_manager.parse_and_filter_log,
            "slaos_key",
        )
    ]

    flow = build_dataflow(
        inputs,  # type: ignore
        OutputTypes.RATED,
        lambda prefix: build_http_sink(output_config, slaos_key=prefix),
    )

    run_main(flow)

    mock_fetch_cloudwatch_logs.assert_called()
    requests = httpx_mock.get_requests()
    assert len(requests) == 1, "There should be 1 request (batched)"

    request = requests[0]
    body = json.loads(request.content)
    assert len(body) == 2, "2 events should have been batched"

    for data in body:
        hashed_secret_key = data["values"]["secret_key"]
        assert isinstance(hashed_secret_key, str)
        assert len(hashed_secret_key) == 64, "Hashed secret_key should be 64 characters"


@patch("src.indexers.dataflow.fetch_metrics")
@patch("src.config.manager.ConfigurationManager.load_config")
def test_metrics_dataflow(
    mock_load_config,
    mock_fetch_metrics,
    httpx_mock: HTTPXMock,
    valid_config_dict,
    mocked_ingestion_endpoint,
):
    config = RatedIndexerYamlConfig(**valid_config_dict)
    config.inputs[0].type = InputTypes.METRICS
    config.inputs[0].integration = IntegrationTypes.DATADOG
    config.inputs[0].datadog = DatadogConfig(
        api_key="your_api_key",
        app_key="your_app_key",
        site="datadog.eu",
        metrics_config=DatadogMetricsConfig(
            metric_name="test.metric",
            interval=60,
            statistic="AVERAGE",
            organization_identifier="customer",
            metric_tag_data=[
                {"customer_value": "customer1", "tag_string": "customer:customer1"},  # type: ignore
                {"customer_value": "customer2", "tag_string": "customer:customer2"},  # type: ignore
            ],
        ),
    )

    mock_load_config.return_value = config

    sample_metrics = [
        {
            "metric_name": "test.metric",
            "organization_id": "customer1",
            "timestamp": 1625097600000,
            "value": 1.0,
        },
        {
            "metric_name": "test.metric",
            "organization_id": "customer1",
            "timestamp": 1625097660000,
            "value": 2.0,
        },
        {
            "metric_name": "test.metric",
            "organization_id": "customer2",
            "timestamp": 1625097720000,
            "value": 3.0,
        },
        {
            "metric_name": "test.metric",
            "organization_id": "customer2",
            "timestamp": 1625097780000,
            "value": 4.0,
        },
    ]
    sample_metric_entries = [
        MetricEntry.from_datadog_metric(metric) for metric in sample_metrics
    ]
    mock_fetch_metrics.return_value = iter(sample_metric_entries)

    output_config = RatedOutputConfig(
        ingestion_id="your_ingestion_id",
        ingestion_key="your_ingestion_key",
        ingestion_url=mocked_ingestion_endpoint,
    )

    mock_input = TestingSource([TimeRange(start_time=1, end_time=2)])
    filter_manager = FilterManager(None, "datadog_slaos_key", InputTypes.METRICS)
    inputs = [
        (
            IntegrationTypes.DATADOG,
            InputTypes.METRICS,
            config.inputs[0].datadog,
            mock_input,
            mock_fetch_metrics,
            filter_manager.parse_and_filter_metrics,
            "datadog_slaos_key",
        )
    ]

    flow = build_dataflow(
        inputs,  # type: ignore
        OutputTypes.RATED,
        lambda prefix: build_http_sink(output_config, slaos_key=prefix),
    )

    run_main(flow)

    mock_fetch_metrics.assert_called()
    requests = httpx_mock.get_requests()
    assert len(requests) == 1, "There should be 1 request (batched)"

    request = requests[0]
    body = json.loads(request.content)
    assert len(body) == 4, "4 events should have been batched"


@patch("src.indexers.dataflow.fetch_logs")
@patch("src.config.manager.ConfigurationManager.load_config")
def test_multiple_inputs_dataflow(
    mock_load_config,
    mock_fetch_logs,
    httpx_mock: HTTPXMock,
    valid_config_dict,
    mocked_ingestion_endpoint,
    redis_container: RedisContainer,
):
    config = RatedIndexerYamlConfig(**valid_config_dict)
    cloudwatch_config = InputYamlConfig(
        slaos_key="cloudwatch_slaos_key",
        type=InputTypes.LOGS,
        integration=IntegrationTypes.CLOUDWATCH,
        cloudwatch=CloudwatchConfig(
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
            region="us-west-2",
        ),
        offset=OffsetYamlConfig(
            type=OffsetTypes.REDIS,
            start_from=123456789,
            start_from_type=StartFromTypes.BIGINT,
            redis=OffsetRedisYamlConfig(
                host="localhost",
                port=int(redis_container.get_exposed_port(6379)),
                db=0,
                key="offset_tracking",
            ),
        ),
        filters=FiltersYamlConfig(
            version=1,
            log_format=LogFormat.JSON,
            log_example={
                "log_level": "INFO",
                "service": "user-auth",
                "event": "login_attempt",
                "user_id": "jsmith123",
                "ip_address": "192.168.1.100",
                "success": "true",
                "duration_ms": 250,
            },
            fields=[
                JsonFieldDefinition(
                    key="log_level",
                    field_type=FieldType.STRING,
                    path="log_level",
                ),
                JsonFieldDefinition(
                    key="service",
                    field_type=FieldType.STRING,
                    path="service",
                ),
                JsonFieldDefinition(
                    key="event",
                    field_type=FieldType.STRING,
                    path="event",
                ),
                JsonFieldDefinition(
                    key="organization_id",
                    field_type=FieldType.STRING,
                    path="user_id",
                ),
                JsonFieldDefinition(
                    key="ip_address",
                    field_type=FieldType.STRING,
                    path="ip_address",
                ),
                JsonFieldDefinition(
                    key="success",
                    field_type=FieldType.STRING,
                    path="success",
                ),
                JsonFieldDefinition(
                    key="duration_ms",
                    field_type=FieldType.INTEGER,
                    path="duration_ms",
                ),
            ],
        ),
    )
    config.inputs = [cloudwatch_config, cloudwatch_config]

    mock_load_config.return_value = config

    sample_logs = [
        {
            "eventId": "log_one",
            "timestamp": 1694390400000,
            "message": '{"log_level": "INFO", "service": "user-auth", "event": "login_attempt", "user_id": "jsmith123", "ip_address": "192.168.1.100", "success": "true", "duration_ms": 250}',
        },
        {
            "eventId": "log_two",
            "timestamp": 1694390401000,
            "message": '{"log_level": "ERROR", "service": "payment", "event": "transaction_failed", "user_id": "asmith456", "ip_address": "192.168.1.101", "error_code": "INSUFFICIENT_FUNDS", "amount": 100.50}',
        },
    ]
    sample_log_entries = [LogEntry.from_cloudwatch_log(log) for log in sample_logs]
    mock_fetch_logs.return_value = iter(sample_log_entries)

    output_config = RatedOutputConfig(
        ingestion_id="your_ingestion_id",
        ingestion_key="your_ingestion_key",
        ingestion_url=mocked_ingestion_endpoint,
    )

    mock_input_logs1 = TestingSource([TimeRange(start_time=1, end_time=2)])
    mock_input_logs2 = TestingSource([TimeRange(start_time=1, end_time=2)])

    filter_manager = FilterManager(
        config.inputs[0].filters, "cloudwatch_slaos_key", InputTypes.LOGS
    )

    inputs = [
        (
            IntegrationTypes.CLOUDWATCH,
            InputTypes.LOGS,
            cloudwatch_config.cloudwatch,
            mock_input_logs1,
            mock_fetch_logs,
            filter_manager.parse_and_filter_log,
            "cloudwatch_slaos_key",
        ),
        (
            IntegrationTypes.CLOUDWATCH,
            InputTypes.LOGS,
            cloudwatch_config.cloudwatch,
            mock_input_logs2,
            mock_fetch_logs,
            filter_manager.parse_and_filter_log,
            "cloudwatch_slaos_key",
        ),
    ]

    flow = build_dataflow(
        inputs,  # type: ignore
        OutputTypes.RATED,
        lambda prefix: build_http_sink(output_config, slaos_key=prefix),
    )

    run_main(flow)

    assert mock_fetch_logs.call_count == 2, "fetch_logs should be called twice"
    requests = httpx_mock.get_requests()
    assert len(requests) == 1, "There should be 1 request (batched)"

    request = requests[0]
    body = json.loads(request.content)
    assert len(body) == 2, "2 events should have been batched (1 log from each input)"

    sent_data = body

    for item in sent_data:
        assert "organization_id" in item, f"Missing 'organization_id' in {item}"
        assert "timestamp" in item, f"Missing 'timestamp' in {item}"
        assert "key" in item, f"Missing 'key' in {item}"
        assert "values" in item, f"Missing 'values' in {item}"
        assert isinstance(
            item["values"], dict
        ), f"'values' is not a dictionary in {item}"


@patch("src.indexers.dataflow.fetch_metrics")
@patch("src.indexers.dataflow.fetch_logs")
@patch("src.config.manager.ConfigurationManager.load_config")
def test_metrics_logs_inputs_dataflow(
    mock_load_config,
    mock_fetch_logs,
    mock_fetch_metrics,
    httpx_mock: HTTPXMock,
    valid_config_dict,
    mocked_ingestion_endpoint,
    redis_container: RedisContainer,
):
    config = RatedIndexerYamlConfig(**valid_config_dict)

    # Configure Datadog metrics input
    datadog_config = InputYamlConfig(
        type=InputTypes.METRICS,
        integration=IntegrationTypes.DATADOG,
        slaos_key="datadog_slaos_key",
        datadog=DatadogConfig(
            api_key="your_api_key",
            app_key="your_app_key",
            site="datadog.eu",
            metrics_config=DatadogMetricsConfig(
                metric_name="test.metric",
                interval=60,
                statistic="AVERAGE",
                organization_identifier="customer",
                metric_tag_data=[
                    {"customer_value": "customer1", "tag_string": "customer:customer1"},  # type: ignore
                    {"customer_value": "customer2", "tag_string": "customer:customer2"},  # type: ignore
                ],
            ),
        ),
        offset=OffsetYamlConfig(
            type=OffsetTypes.REDIS,
            start_from=123456789,
            start_from_type=StartFromTypes.BIGINT,
            redis=OffsetRedisYamlConfig(
                host="localhost",
                port=int(redis_container.get_exposed_port(6379)),
                db=0,
                key="offset_tracking",
            ),
        ),
        filters=None,
    )

    # Configure CloudWatch logs input
    cloudwatch_config = InputYamlConfig(
        type=InputTypes.LOGS,
        integration=IntegrationTypes.CLOUDWATCH,
        slaos_key="cloudwatch_slaos_key",
        cloudwatch=CloudwatchConfig(
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
            region="us-west-2",
        ),
        offset=OffsetYamlConfig(
            type=OffsetTypes.REDIS,
            start_from=123456789,
            start_from_type=StartFromTypes.BIGINT,
            redis=OffsetRedisYamlConfig(
                host="localhost",
                port=int(redis_container.get_exposed_port(6379)),
                db=0,
                key="offset_tracking",
            ),
        ),
        filters=FiltersYamlConfig(
            version=1,
            log_format=LogFormat.JSON,
            log_example={
                "log_level": "INFO",
                "service": "user-auth",
                "event": "login_attempt",
                "user_id": "jsmith123",
                "ip_address": "192.168.1.100",
                "success": "true",
                "duration_ms": 250,
            },
            fields=[
                JsonFieldDefinition(
                    key="log_level", field_type=FieldType.STRING, path="log_level"
                ),
                JsonFieldDefinition(
                    key="service", field_type=FieldType.STRING, path="service"
                ),
                JsonFieldDefinition(
                    key="event", field_type=FieldType.STRING, path="event"
                ),
                JsonFieldDefinition(
                    key="organization_id", field_type=FieldType.STRING, path="user_id"
                ),
                JsonFieldDefinition(
                    key="ip_address", field_type=FieldType.STRING, path="ip_address"
                ),
                JsonFieldDefinition(
                    key="success", field_type=FieldType.STRING, path="success"
                ),
                JsonFieldDefinition(
                    key="duration_ms",
                    field_type=FieldType.INTEGER,
                    path="duration_ms",
                    hash=True,
                ),
            ],
        ),
    )

    config.inputs = [cloudwatch_config, datadog_config]
    mock_load_config.return_value = config

    # Mock Datadog metrics
    sample_metrics = [
        {
            "metric_name": "test.metric",
            "organization_id": "customer1",
            "timestamp": 1625097600000,
            "value": 1.0,
        },
        {
            "metric_name": "test.metric",
            "organization_id": "customer1",
            "timestamp": 1625097660000,
            "value": 2.0,
        },
        {
            "metric_name": "test.metric",
            "organization_id": "customer2",
            "timestamp": 1625097720000,
            "value": 3.0,
        },
        {
            "metric_name": "test.metric",
            "organization_id": "customer2",
            "timestamp": 1625097780000,
            "value": 4.0,
        },
    ]
    sample_metric_entries = [
        MetricEntry.from_datadog_metric(metric) for metric in sample_metrics
    ]
    mock_fetch_metrics.return_value = iter(sample_metric_entries)

    # Mock CloudWatch logs (using JSON directly)
    sample_logs = [
        {
            "eventId": "log_one",
            "timestamp": 1694390400000,
            "message": '{"log_level": "INFO", "service": "user-auth", "event": "login_attempt", "user_id": "jsmith123", "ip_address": "192.168.1.100", "success": "true", "duration_ms": 250}',
        },
        {
            "eventId": "log_two",
            "timestamp": 1694390401000,
            "message": '{"log_level": "ERROR", "service": "payment", "event": "transaction_failed", "user_id": "asmith456", "ip_address": "192.168.1.101", "error_code": "INSUFFICIENT_FUNDS", "amount": 100.50}',
        },
    ]
    sample_log_entries = [LogEntry.from_cloudwatch_log(log) for log in sample_logs]
    mock_fetch_logs.return_value = iter(sample_log_entries)

    output_config = RatedOutputConfig(
        ingestion_id="your_ingestion_id",
        ingestion_key="your_ingestion_key",
        ingestion_url=mocked_ingestion_endpoint,
    )

    mock_input_metrics = TestingSource([TimeRange(start_time=1, end_time=2)])
    mock_input_logs = TestingSource([TimeRange(start_time=1, end_time=2)])

    filter_manager_logs = FilterManager(
        config.inputs[0].filters, "cloudwatch_slaos_key", InputTypes.LOGS
    )
    filter_manager_metrics = FilterManager(
        config.inputs[1].filters, "datadog_slaos_key", InputTypes.METRICS
    )

    inputs = [
        (
            IntegrationTypes.DATADOG,
            InputTypes.METRICS,
            datadog_config.datadog,
            mock_input_metrics,
            mock_fetch_metrics,
            filter_manager_metrics.parse_and_filter_metrics,
            "datadog_slaos_key",
        ),
        (
            IntegrationTypes.CLOUDWATCH,
            InputTypes.LOGS,
            cloudwatch_config.cloudwatch,
            mock_input_logs,
            mock_fetch_logs,
            filter_manager_logs.parse_and_filter_log,
            "cloudwatch_slaos_key",
        ),
    ]

    flow = build_dataflow(
        inputs,  # type: ignore
        config.output.type,
        lambda prefix: build_http_sink(output_config, slaos_key=prefix),
    )

    run_main(flow)

    assert (
        mock_fetch_metrics.call_count == 1
    ), f"fetch_metrics should be called once, called {mock_fetch_metrics.call_count}"
    assert (
        mock_fetch_logs.call_count == 1
    ), f"fetch_logs should be called once,  called {mock_fetch_metrics.call_count}"

    requests = httpx_mock.get_requests()
    assert len(requests) == 1, "There should be 1 request (batched)"

    request = requests[0]
    body = json.loads(request.content)

    metric_count = sum(1 for item in body if "test_metric" in item["values"])
    log_count = sum(1 for item in body if "log_level" in item["values"])
    assert metric_count == 4, f"Expected 4 metrics, got {metric_count}"
    assert log_count == 2, f"Expected 2 logs, got {log_count}"

    sent_data = body
    for item in sent_data:
        assert "organization_id" in item, f"Missing 'organization_id' in {item}"
        assert "timestamp" in item, f"Missing 'timestamp' in {item}"
        assert "key" in item, f"Missing 'key' in {item}"
        assert "values" in item, f"Missing 'values' in {item}"
        assert isinstance(
            item["values"], dict
        ), f"'values' is not a dictionary in {item}"

        if "duration_ms" in item["values"]:
            assert (
                len(item["values"]["duration_ms"]) == 64
            ), "Hashed duration_ms should be 64 characters"


def test_metrics_interval(
    mock_load_config,
    mock_time,
    mock_get_offset_tracker,
    valid_prometheus_config_dict,
    mock_prometheus_query,
):
    valid_config = RatedIndexerYamlConfig(**valid_prometheus_config_dict)
    mock_load_config.return_value = valid_config

    # Create partition directly to test intervals
    partition = RatedPartition("prometheus_metrics", 0)

    # Initial batch - should be MAX interval for catch-up
    first_batch = partition.next_batch()
    assert len(first_batch) == 1
    assert (
        first_batch[0].end_time - first_batch[0].start_time
    ) == FetchInterval.MAX.to_milliseconds()

    # Move time forward
    new_time = mock_time.now.return_value + timedelta(
        seconds=float(FetchInterval.METRICS)
    )
    mock_time.now.return_value = new_time

    # Next batch should use METRICS interval
    second_batch = partition.next_batch()
    assert len(second_batch) == 1
    assert (
        second_batch[0].end_time - second_batch[0].start_time
    ) <= FetchInterval.METRICS.to_milliseconds()

    # Verify next_awake timing
    expected_wake = new_time + timedelta(seconds=float(FetchInterval.METRICS))
    assert abs(partition.next_awake().timestamp() - expected_wake.timestamp()) < 1
