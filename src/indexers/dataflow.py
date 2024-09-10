from typing import Callable, Iterator, Union, Optional, List, Tuple

from bytewax.dataflow import Dataflow, Stream
import bytewax.operators as op
from bytewax.inputs import FixedPartitionedSource
from bytewax.outputs import DynamicSink

from src.clients.datadog import get_datadog_client, DatadogClient
from src.clients.cloudwatch import get_cloudwatch_client, CloudwatchClient
from src.indexers.filters.types import LogEntry, MetricEntry
from src.indexers.filters.manager import FilterManager
from src.config.manager import RatedIndexerYamlConfig
from src.config.models.inputs.input import IntegrationTypes, InputTypes
from src.config.models.output import OutputTypes
from src.indexers.sinks.console import build_console_sink
from src.indexers.sinks.rated import build_http_sink
from src.indexers.sources.rated import RatedSource, TimeRange
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
    List[
        Tuple[IntegrationTypes, InputTypes, FixedPartitionedSource, Callable, Callable]
    ],
    OutputTypes,
    DynamicSink,
]:
    inputs = []
    for input_config in config.inputs:
        input_source = RatedSource()
        fetcher = fetch_logs if input_config.type == InputTypes.LOGS else fetch_metrics
        filter_manager = FilterManager(input_config.filters)
        filter_logic = (
            filter_manager.parse_and_filter_log
            if input_config.type == InputTypes.LOGS
            else filter_manager.parse_and_filter_metrics
        )
        inputs.append(
            (
                input_config.integration,
                input_config.type,
                input_source,
                fetcher,
                filter_logic,
            )
        )

    output_config = config.output

    if output_config.type == OutputTypes.RATED and output_config.rated:
        rated_config = output_config.rated
        output_sink = build_http_sink(rated_config.ingestion_url)  # type: ignore
    elif output_config.type == OutputTypes.CONSOLE:
        output_sink = build_console_sink()  # type: ignore
    else:
        raise ValueError(f"Invalid output source: {output_config.type}")

    return inputs, output_config.type, output_sink  # type: ignore


def build_dataflow(
    inputs: List[
        Tuple[IntegrationTypes, InputTypes, FixedPartitionedSource, Callable, Callable]
    ],
    output_type: OutputTypes,
    output_sink: DynamicSink,
) -> Dataflow:
    logger.info(f"Building indexer dataflow for {len(inputs)} inputs")

    flow = Dataflow("rated_multi_input_indexer")

    output_streams = []

    for idx, (
        integration_type,
        input_type,
        input_source,
        fetcher,
        filter_logic,
    ) in enumerate(inputs):
        stream: Stream = (
            op.input(f"input_source_{idx}", flow, input_source)
            .then(
                op.flat_map,
                f"fetch_{integration_type.value}_{input_type.value}_{idx}",
                lambda x: fetcher(x, integration_type),
            )
            .then(op.filter_map, f"filter_{input_type.value}_{idx}", filter_logic)
        )
        output_streams.append(stream)

    if len(output_streams) > 1:
        merged_stream = op.merge("merge_streams", *output_streams)
    else:
        merged_stream = output_streams[0]

    merged_stream.then(op.output, f"{output_type.value}_output_sink", output_sink)

    return flow


def dataflow(config: RatedIndexerYamlConfig) -> Dataflow:
    inputs, output_type, output_sink = parse_config(config)

    flow = build_dataflow(
        inputs,
        output_type,
        output_sink,
    )
    return flow
