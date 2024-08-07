from datetime import datetime
from typing import Callable

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.inputs import FixedPartitionedSource
from bytewax.outputs import DynamicSink

from src.config.manager import RatedIndexerYamlConfig
from src.config.models.input import InputTypes
from src.config.models.output import OutputTypes
from src.indexers.sinks.console import build_console_sink
from src.indexers.sinks.rated import build_http_sink
from src.indexers.sources.cloudwatch import CloudwatchSource, fetch_cloudwatch_logs
from src.utils.logger import logger
from src.utils.time_conversion import to_milliseconds


def parse_config(
    config: RatedIndexerYamlConfig,
) -> tuple[InputTypes, FixedPartitionedSource, Callable, OutputTypes, DynamicSink]:
    input_config = config.input
    output_config = config.output
    offset_config = config.offset

    if input_config.type == InputTypes.CLOUDWATCH.value and input_config.cloudwatch:
        start_time = (
            to_milliseconds(offset_config.start_from)
            if isinstance(offset_config.start_from, datetime)
            else offset_config.start_from
        )
        input_source = CloudwatchSource(start_from=start_time)
        logs_fetcher = fetch_cloudwatch_logs
    else:
        raise ValueError(f"Invalid input source: {input_config.type}")

    if output_config.type == OutputTypes.RATED.value and output_config.rated:
        rated_config = output_config.rated
        output_sink = build_http_sink(rated_config.ingestion_url)  # type: ignore
    elif output_config.type == OutputTypes.CONSOLE.value:
        output_sink = build_console_sink()  # type: ignore
    else:
        raise ValueError(f"Invalid output source: {output_config.type}")

    return (
        input_config.type,
        input_source,
        logs_fetcher,
        output_config.type,
        output_sink,
    )


def build_dataflow(
    input_type: InputTypes,
    input_source: FixedPartitionedSource,
    fetch_logs: Callable,
    output_type: OutputTypes,
    output_source: DynamicSink,
) -> Dataflow:
    logger.info("Building indexer dataflow")

    flow = Dataflow("rated_logs_indexer")
    (
        op.input(f"{input_type.value}_input_source", flow, input_source)
        .then(op.flat_map, f"fetch_{input_type.value}_logs", fetch_logs)
        .then(op.output, f"{output_type.value}_output_sink", output_source)
    )
    return flow


def dataflow(config: RatedIndexerYamlConfig) -> Dataflow:
    (input_type, input_source, logs_fetcher, output_type, output_sink) = parse_config(
        config
    )

    flow = build_dataflow(
        input_type, input_source, logs_fetcher, output_type, output_sink
    )
    return flow
