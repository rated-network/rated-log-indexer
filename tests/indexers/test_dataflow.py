from unittest.mock import patch

from bytewax.testing import run_main, TestingSource
from pytest_httpx import HTTPXMock
from rated_parser.payloads.inputs import JsonFieldDefinition, LogFormat, FieldType  # type: ignore

from src.config.models.inputs.datadog import (
    DatadogConfig,
    DatadogMetricsConfig,
    DatadogTag,
)
from src.indexers.filters.types import LogEntry, MetricEntry
from src.config.models.filters import FiltersYamlConfig
from src.indexers.filters.manager import FilterManager, parse_and_filter_metrics
from src.config.models.output import RatedOutputConfig
from src.config.models.inputs.input import IntegrationTypes, InputTypes
from src.config.models.output import OutputTypes
from src.indexers.sinks.rated import build_http_sink
from src.indexers.sources.rated import TimeRange
from src.config.manager import RatedIndexerYamlConfig
from src.indexers.dataflow import build_dataflow


@patch("src.indexers.dataflow.fetch_logs")
@patch("src.config.manager.ConfigurationManager.load_config")
def test_logs_dataflow(
    mock_load_config,
    mock_fetch_cloudwatch_logs,
    httpx_mock: HTTPXMock,
    valid_config_dict,
):
    mock_load_config.return_value = RatedIndexerYamlConfig(**valid_config_dict)
    sample_logs = [
        {
            "eventId": "mock_log_one",
            "timestamp": 1723041096000,
            "message": '{"example_key": "example_value_one", "data": {"customer_id": "customer_one"}}',
        },
        {
            "eventId": "mock_log_two",
            "timestamp": 1723041096100,
            "message": '{"example_key": "example_value_two", "data": {"customer_id": "customer_two"}}',
        },
    ]
    sample_log_entries = [LogEntry.from_cloudwatch_log(log) for log in sample_logs]
    mock_fetch_cloudwatch_logs.return_value = iter(sample_log_entries)

    endpoint = "https://your_ingestion_url.com"
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
    filter_config = FiltersYamlConfig(
        version=1,
        log_format=LogFormat.JSON,
        log_example={
            "message": {
                "example_key": "example_value_two",
                "customer_id": "customer_two",
            },
        },
        fields=[
            JsonFieldDefinition(
                key="example_key",
                field_type=FieldType.STRING,
                path="example_key",
            ),
            JsonFieldDefinition(
                key="customer_id",
                field_type=FieldType.STRING,
                path="data.customer_id",
            ),
        ],
    )
    filter_manager = FilterManager(filter_config)

    mock_input = TestingSource([TimeRange(start_time=1, end_time=2)])
    flow = build_dataflow(
        IntegrationTypes.CLOUDWATCH,
        InputTypes.LOGS,
        mock_input,
        mock_fetch_cloudwatch_logs,
        OutputTypes.RATED,
        build_http_sink(output_config),
        filter_manager.parse_and_filter_log,
    )

    run_main(flow)

    mock_fetch_cloudwatch_logs.assert_called()
    requests = httpx_mock.get_requests()
    assert len(requests) == 2


@patch("src.indexers.dataflow.fetch_metrics")
@patch("src.config.manager.ConfigurationManager.load_config")
def test_metrics_dataflow(
    mock_load_config,
    mock_fetch_metrics,
    httpx_mock: HTTPXMock,
    valid_config_dict,
):
    config = RatedIndexerYamlConfig(**valid_config_dict)
    config.input.type = InputTypes.METRICS
    config.input.integration = IntegrationTypes.DATADOG
    config.input.datadog = DatadogConfig(
        api_key="your_api_key",
        app_key="your_app_key",
        site="datadog.eu",
        metrics_config=DatadogMetricsConfig(
            metric_name="test.metric",
            interval=60,
            statistic="AVERAGE",
            customer_identifier="customer",
            metric_tag_data=[
                DatadogTag(customer_value="customer1", tag_string="customer:customer1"),
                DatadogTag(customer_value="customer2", tag_string="customer:customer2"),
            ],
        ),
    )

    mock_load_config.return_value = config

    sample_metrics = [
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
            "customer_id": "customer2",
            "timestamp": 1625097720000,
            "value": 3.0,
        },
        {
            "metric_name": "test.metric",
            "customer_id": "customer2",
            "timestamp": 1625097780000,
            "value": 4.0,
        },
    ]
    sample_metric_entries = [
        MetricEntry.from_datadog_metric(metric) for metric in sample_metrics
    ]
    mock_fetch_metrics.return_value = iter(sample_metric_entries)

    endpoint = "https://your_ingestion_url.com"
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

    mock_input = TestingSource([TimeRange(start_time=1, end_time=2)])
    flow = build_dataflow(
        IntegrationTypes.DATADOG,
        InputTypes.METRICS,
        mock_input,
        mock_fetch_metrics,
        OutputTypes.RATED,
        build_http_sink(output_config),
        parse_and_filter_metrics,
    )

    run_main(flow)

    mock_fetch_metrics.assert_called()
    requests = httpx_mock.get_requests()
    assert len(requests) == 4
