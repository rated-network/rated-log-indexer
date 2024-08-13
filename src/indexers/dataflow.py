from typing import Callable, Iterator

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.inputs import FixedPartitionedSource
from bytewax.outputs import DynamicSink

from src.clients.cloudwatch import get_cloudwatch_client
from src.indexers.filters.types import LogEntry
from src.indexers.filters.manager import FilterManager
from src.config.manager import RatedIndexerYamlConfig
from src.config.models.input import IntegrationTypes
from src.config.models.output import OutputTypes
from src.indexers.sinks.console import build_console_sink
from src.indexers.sinks.rated import build_http_sink
from src.indexers.sources.logs import LogsSource, TimeRange
from src.utils.logger import logger


cloudwatch_client = None


def get_cloudwatch_client_instance():
    global cloudwatch_client
    if cloudwatch_client is None:
        cloudwatch_client = get_cloudwatch_client()
    return cloudwatch_client


def fetch_cloudwatch_logs(
    time_range: TimeRange,
) -> Iterator[LogEntry]:
    raw_logs = get_cloudwatch_client_instance().query_logs(
        time_range.start_time, time_range.end_time
    )
    return (LogEntry.from_cloudwatch_log(log) for log in raw_logs)


def parse_config(
    config: RatedIndexerYamlConfig,
) -> tuple[
    IntegrationTypes,
    FixedPartitionedSource,
    Callable,
    OutputTypes,
    DynamicSink,
    FilterManager,
]:
    input_config = config.input
    output_config = config.output
    filter_config = config.filters

    input_source = LogsSource()

    if (
        input_config.integration == IntegrationTypes.CLOUDWATCH.value
        and input_config.cloudwatch
    ):
        logs_fetcher = fetch_cloudwatch_logs
    else:
        raise ValueError(f"Invalid input source: {input_config.integration}")

    if output_config.type == OutputTypes.RATED.value and output_config.rated:
        rated_config = output_config.rated
        output_sink = build_http_sink(rated_config.ingestion_url)  # type: ignore
    elif output_config.type == OutputTypes.CONSOLE.value:
        output_sink = build_console_sink()  # type: ignore
    else:
        raise ValueError(f"Invalid output source: {output_config.type}")

    return (
        input_config.integration,
        input_source,
        logs_fetcher,
        output_config.type,
        output_sink,
        FilterManager(filter_config),
    )


def build_dataflow(
    input_type: IntegrationTypes,
    input_source: FixedPartitionedSource,
    fetch_logs: Callable,
    output_type: OutputTypes,
    output_source: DynamicSink,
    filter_manager: FilterManager,
) -> Dataflow:
    logger.info("Building indexer dataflow")

    flow = Dataflow("rated_logs_indexer")
    (
        op.input(f"{input_type.value}_input_source", flow, input_source)
        .then(op.flat_map, f"fetch_{input_type.value}_logs", fetch_logs)
        .then(op.filter_map, "filter_logs", filter_manager.parse_and_filter_log)
        .then(op.output, f"{output_type.value}_output_sink", output_source)
    )
    return flow


def dataflow(config: RatedIndexerYamlConfig) -> Dataflow:
    (
        input_type,
        input_source,
        logs_fetcher,
        output_type,
        output_sink,
        filter_manager,
    ) = parse_config(config)

    flow = build_dataflow(
        input_type, input_source, logs_fetcher, output_type, output_sink, filter_manager
    )
    return flow
