from collections import defaultdict
from typing import Callable, Iterator, Union, List, Tuple, Protocol

import structlog
from bytewax.dataflow import Dataflow, Stream
import bytewax.operators as op
from bytewax.inputs import FixedPartitionedSource
from bytewax.outputs import DynamicSink
from pydantic import StrictStr

from src.clients.manager import ClientManager, ClientTypes, ClientConfigTypes
from src.indexers.filters.types import LogEntry, MetricEntry
from src.indexers.filters.manager import FilterManager
from src.config.manager import RatedIndexerYamlConfig
from src.config.models.inputs.input import IntegrationTypes, InputTypes
from src.config.models.output import OutputTypes
from src.indexers.sinks.console import build_console_sink
from src.indexers.sinks.rated import build_http_sink
from src.indexers.sources.rated import RatedSource, TimeRange


logger = structlog.get_logger(__name__)

client_manager = ClientManager()


class DataFetcher(Protocol):
    def __call__(
        self,
        time_range: TimeRange,
        integration_id: StrictStr,
        integration_type: IntegrationTypes,
    ) -> Iterator[Union[LogEntry, MetricEntry]]: ...


class FilterLogic(Protocol):
    def __call__(
        self, entry: Union[LogEntry, MetricEntry]
    ) -> Union[LogEntry, MetricEntry, None]: ...


def get_client_instance(client_id: StrictStr) -> ClientTypes:
    client = client_manager.get_client(client_id)
    if client is None:
        raise ValueError(f"No client found for client_id: {client_id}")
    return client


def fetch_logs(
    time_range: TimeRange, integration_id: StrictStr, integration_type: IntegrationTypes
) -> Iterator[LogEntry]:
    client = get_client_instance(integration_id)

    if integration_type == IntegrationTypes.CLOUDWATCH.value:
        raw_logs = client.query_logs(time_range.start_time, time_range.end_time)
        return (LogEntry.from_cloudwatch_log(log) for log in raw_logs)
    elif integration_type == IntegrationTypes.DATADOG.value:
        raw_logs = client.query_logs(time_range.start_time, time_range.end_time)
        return (LogEntry.from_datadog_log(log) for log in raw_logs)
    else:
        raise ValueError(f"Unsupported integration type: {integration_type}")


def fetch_metrics(
    time_range: TimeRange, integration_id: StrictStr, integration_type: IntegrationTypes
) -> Iterator[MetricEntry]:
    client = get_client_instance(integration_id)

    if integration_type == IntegrationTypes.CLOUDWATCH.value:
        raw_metrics = client.query_metrics(time_range.start_time, time_range.end_time)
        return (MetricEntry.from_cloudwatch_metric(metric) for metric in raw_metrics)
    elif integration_type == IntegrationTypes.DATADOG.value:
        raw_metrics = client.query_metrics(time_range.start_time, time_range.end_time)
        return (MetricEntry.from_datadog_metric(metric) for metric in raw_metrics)
    elif integration_type == IntegrationTypes.PROMETHEUS.value:
        raw_metrics = client.query_metrics(time_range.start_time, time_range.end_time)
        return (MetricEntry.from_prometheus_metric(metric) for metric in raw_metrics)
    else:
        raise ValueError(f"Unsupported integration type: {integration_type}")


def parse_config(
    config: RatedIndexerYamlConfig,
) -> Tuple[
    List[
        Tuple[
            IntegrationTypes,
            InputTypes,
            ClientConfigTypes,
            FixedPartitionedSource,
            DataFetcher,
            FilterLogic,
            str,
        ]
    ],
    OutputTypes,
    Callable[[str], DynamicSink],
]:
    inputs = []
    slaos_key_count: defaultdict = defaultdict(int)

    for input_config in config.inputs:
        slaos_key = input_config.slaos_key
        config_index = slaos_key_count[slaos_key]
        slaos_key_count[slaos_key] += 1

        input_source = RatedSource(slaos_key=slaos_key, config_index=config_index)

        client_config: ClientConfigTypes = getattr(
            input_config, input_config.integration.value.lower()
        )

        fetcher = fetch_logs if input_config.type == InputTypes.LOGS else fetch_metrics
        filter_manager = FilterManager(
            input_config.filters, input_config.slaos_key, input_config.type
        )

        filter_logic = (
            filter_manager.parse_and_filter_log
            if input_config.type == InputTypes.LOGS
            else filter_manager.parse_and_filter_metrics
        )
        inputs.append(
            (
                input_config.integration,
                input_config.type,
                client_config,
                input_source,
                fetcher,
                filter_logic,
                input_config.slaos_key,
            )
        )

    output_config = config.output

    if output_config.type == OutputTypes.RATED and output_config.rated:
        rated_config = output_config.rated

        def output_sink_builder(prefix: str) -> DynamicSink:
            return build_http_sink(rated_config, prefix)

    elif output_config.type == OutputTypes.CONSOLE:

        def output_sink_builder(prefix: str) -> DynamicSink:
            return build_console_sink()

    else:
        raise ValueError(f"Invalid output source: {output_config.type}")

    return inputs, output_config.type, output_sink_builder  # type: ignore


def build_dataflow(
    inputs: List[
        Tuple[
            IntegrationTypes,
            InputTypes,
            ClientConfigTypes,
            FixedPartitionedSource,
            DataFetcher,
            FilterLogic,
            str,
        ]
    ],
    output_type: OutputTypes,
    output_sink_builder: Callable[[str], DynamicSink],
) -> Dataflow:
    logger.info(f"Building indexer dataflow for {len(inputs)} inputs")

    flow = Dataflow("rated_multi_input_indexer")

    output_streams = []

    for idx, (
        integration_type,
        input_type,
        client_config,
        input_source,
        fetcher,
        filter_logic,
        slaos_key,
    ) in enumerate(inputs):
        logger.info(
            f"Building stream {idx} for {integration_type} {input_type} with prefix '{slaos_key}'"
        )

        client_id = client_manager.add_client(integration_type, client_config)

        def create_fetcher(f, client, integration):
            def wrapped_fetcher(x):
                result = f(x, client, integration)
                return result

            return wrapped_fetcher

        def create_filter(f, prefix):
            def wrapped_filter(x):
                result = f(x)
                if result:
                    result.slaos_key = prefix
                return result

            return wrapped_filter

        stream: Stream = (
            op.input(f"input_source_{idx}", flow, input_source)
            .then(
                op.flat_map,
                f"fetch_{integration_type.value}_{input_type.value}_{idx}",
                create_fetcher(fetcher, client_id, integration_type),
            )
            .then(
                op.filter_map,
                f"filter_{input_type.value}_{idx}",
                create_filter(filter_logic, slaos_key),
            )
        )
        output_streams.append(stream)

    if len(output_streams) > 1:
        logger.info("Merging streams")
        merged_stream = op.merge("merge_streams", *output_streams)
    else:
        logger.info("Using single stream")
        merged_stream = output_streams[0]

    logger.info(f"Adding output sink: {output_type.value}")

    merged_stream.then(op.output, "sink_output", output_sink_builder(""))

    return flow


def dataflow(config: RatedIndexerYamlConfig) -> Dataflow:
    inputs, output_type, output_sink_builder = parse_config(config)

    flow = build_dataflow(
        inputs,
        output_type,
        output_sink_builder,
    )

    return flow
