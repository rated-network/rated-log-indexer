from typing import Callable, Iterator, Union, Optional

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.inputs import FixedPartitionedSource
from bytewax.outputs import DynamicSink

from src.clients.datadog import get_datadog_client, DatadogClient
from src.clients.cloudwatch import get_cloudwatch_client, CloudwatchClient
from src.indexers.filters.types import LogEntry, MetricEntry
from src.indexers.filters.manager import FilterManager, parse_and_filter_metrics
from src.config.manager import RatedIndexerYamlConfig
from src.config.models.inputs.input import IntegrationTypes, InputTypes
from src.config.models.output import OutputTypes
from src.indexers.sinks.console import build_console_sink
from src.indexers.sinks.rated import build_http_sink
from src.indexers.sources.logs import LogsSource, TimeRange
from src.utils.logger import logger


integration_client: Optional[Union[DatadogClient, CloudwatchClient]] = None


def get_client_instance(
    integration_type: IntegrationTypes,
) -> Union[CloudwatchClient, DatadogClient]:
    global integration_client

    if integration_type == IntegrationTypes.CLOUDWATCH.value:
        if integration_client is None:
            integration_client = get_cloudwatch_client()
        return integration_client

    elif integration_type == IntegrationTypes.DATADOG.value:
        if integration_client is None:
            integration_client = get_datadog_client()
        return integration_client

    else:
        raise ValueError(f"Unsupported integration type: {integration_type}")


def fetch_logs(
    time_range: TimeRange, integration_type: IntegrationTypes
) -> Iterator[LogEntry]:
    client = get_client_instance(integration_type)

    if integration_type == IntegrationTypes.CLOUDWATCH.value:
        raw_logs = client.query_logs(time_range.start_time, time_range.end_time)
        return (LogEntry.from_cloudwatch_log(log) for log in raw_logs)
    elif integration_type == IntegrationTypes.DATADOG.value:
        raw_logs = client.query_logs(time_range.start_time, time_range.end_time)
        return (LogEntry.from_datadog_log(log) for log in raw_logs)
    else:
        raise ValueError(f"Unsupported integration type: {integration_type}")


def fetch_metrics(
    time_range: TimeRange, integration_type: IntegrationTypes
) -> Iterator[MetricEntry]:
    client = get_client_instance(integration_type)

    if integration_type == IntegrationTypes.CLOUDWATCH.value:
        raw_metrics = client.query_metrics(time_range.start_time, time_range.end_time)
        return (MetricEntry.from_cloudwatch_metric(metric) for metric in raw_metrics)
    elif integration_type == IntegrationTypes.DATADOG.value:
        raw_metrics = client.query_metrics(time_range.start_time, time_range.end_time)
        return (MetricEntry.from_datadog_metric(metric) for metric in raw_metrics)
    else:
        raise ValueError(f"Unsupported integration type: {integration_type}")


def parse_config(
    config: RatedIndexerYamlConfig,
) -> tuple[
    IntegrationTypes,
    InputTypes,
    FixedPartitionedSource,
    Callable,
    OutputTypes,
    DynamicSink,
    Callable,
]:
    input_config = config.input
    output_config = config.output
    filter_config = config.filters

    input_source = LogsSource()
    filter_logic = (
        FilterManager(filter_config).parse_and_filter_log
        if input_config.type == InputTypes.LOGS.value
        else parse_and_filter_metrics
    )
    fetcher = (
        fetch_logs if input_config.type == InputTypes.LOGS.value else fetch_metrics
    )

    if output_config.type == OutputTypes.RATED.value and output_config.rated:
        rated_config = output_config.rated
        output_sink = build_http_sink(rated_config.ingestion_url)  # type: ignore
    elif output_config.type == OutputTypes.CONSOLE.value:
        output_sink = build_console_sink()  # type: ignore
    else:
        raise ValueError(f"Invalid output source: {output_config.type}")

    return (
        input_config.integration,
        input_config.type,
        input_source,
        fetcher,
        output_config.type,
        output_sink,
        filter_logic,
    )


def build_dataflow(
    integration_type: IntegrationTypes,
    input_type: InputTypes,
    input_source: FixedPartitionedSource,
    fetcher: Callable,
    output_type: OutputTypes,
    output_source: DynamicSink,
    filter_logic: Callable,
) -> Dataflow:
    logger.info(
        rf"Building indexer dataflow for {integration_type.value} {input_type.value}"
    )

    flow = Dataflow("rated_logs_indexer")
    (
        op.input(f"{integration_type.value}_input_source", flow, input_source)
        .then(
            op.flat_map,
            f"fetch_{integration_type.value}_{input_type.value}",
            lambda x: fetcher(x, integration_type),
        )
        .then(op.filter_map, f"filter_{input_type.value}", filter_logic)
        .then(op.output, f"{output_type.value}_output_sink", output_source)
    )
    return flow


def dataflow(config: RatedIndexerYamlConfig) -> Dataflow:
    (
        integration_type,
        input_type,
        input_source,
        logs_fetcher,
        output_type,
        output_sink,
        filter_logic,
    ) = parse_config(config)

    flow = build_dataflow(
        integration_type,
        input_type,
        input_source,
        logs_fetcher,
        output_type,
        output_sink,
        filter_logic,
    )
    return flow
