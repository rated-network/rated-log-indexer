from enum import Enum
from typing import Any, Dict, Iterator, Optional, List, Tuple

import stamina
import structlog
from boto3 import client  # type: ignore
from botocore.client import BaseClient  # type: ignore
from botocore.config import Config  # type: ignore
from pydantic import PositiveInt

from src.config.manager import ConfigurationManager
from src.config.models.inputs.cloudwatch import (
    CloudwatchConfig,
    CloudwatchMetricsConfig,
)
from src.utils.time_conversion import from_milliseconds

logger = structlog.get_logger("cloudwatch_client")


class CloudwatchClientError(Exception):
    """Custom exception for Cloudwatch Client errors."""

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class ClientType(Enum):
    LOGS = "logs"
    CLOUDWATCH = "cloudwatch"


class QueryLimit(Enum):
    LOGS = 10_000
    METRICS = 100_001  # 100_000 is the maximum number of data points we are querying in a single request


class CloudwatchClient:
    def __init__(self, config: CloudwatchConfig, limit: Optional[PositiveInt] = None):
        self.config = config

        self.logs_client = self._get_client(ClientType.LOGS)
        self.logs_query_limit = limit if limit else QueryLimit.LOGS.value

        self.metrics_client = self._get_client(ClientType.CLOUDWATCH)
        self.metrics_query_limit = limit if limit else QueryLimit.METRICS.value
        self.metrics_query_chunk_size = 500

    def _get_client(self, client_type: ClientType) -> BaseClient:
        return client(
            client_type.value,
            config=Config(
                region_name=self.config.region,
            ),
            aws_access_key_id=self.config.aws_access_key_id,
            aws_secret_access_key=self.config.aws_secret_access_key,
        )

    @stamina.retry(on=Exception, attempts=5)
    def query_logs(
        self,
        start_time: PositiveInt,
        end_time: PositiveInt,
    ) -> Iterator[Dict[str, Any]]:
        """
        Fetch logs from the specified AWS CloudWatch log group within the given time range.

        This method retrieves logs between the `start_time` and `end_time` and returns them
        as an iterator of dictionaries. It automatically retries up to 5 times if any
        exceptions occur during the log fetching process.

        Args:
            start_time (PositiveInt): The start time in milliseconds since the Unix epoch
                                      from which to begin fetching logs.
            end_time (PositiveInt): The end time in milliseconds since the Unix epoch
                                    up to which logs should be fetched.

        Returns:
            Iterator[Dict[str, Any]]: An iterator that yields log entries as dictionaries.

        Raises:
            Exception: If the log fetching operation fails after 5 attempts.
        """
        logs_config = self.config.logs_config

        if not logs_config:
            msg = "Cloudwatch logs configuration is missing."
            logger.error(msg, exc_info=True)
            raise CloudwatchClientError(msg)

        filter_pattern = logs_config.filter_pattern
        params = {
            "logGroupName": logs_config.log_group_name,
            "startTime": start_time,
            "endTime": end_time,
            "limit": self.logs_query_limit,
        }

        if filter_pattern:
            params["filterPattern"] = filter_pattern

        next_token = None

        while True:
            if next_token:
                params["nextToken"] = next_token

            try:
                events_batch = self.logs_client.filter_log_events(**params)
                logs = events_batch.get("events", [])
                logger.info(
                    f"Fetched {len(logs)} logs from Cloudwatch",
                    start_time=start_time,
                    end_time=end_time,
                    log_group_name=logs_config.log_group_name,
                    start_time_str=from_milliseconds(start_time).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                    end_time_str=from_milliseconds(end_time).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                )
                yield from logs

                if len(logs) < self.logs_query_limit:
                    break

                next_token = events_batch.get("nextToken")
                if not next_token:
                    break
            except Exception as e:
                msg = f"Failed to query logs: {e}"
                logger.error(msg, exc_info=True)
                raise CloudwatchClientError(msg)

    def _parse_metrics_queries(
        self, metrics_config: CloudwatchMetricsConfig
    ) -> Tuple[Dict[str, Any], List[List[Dict[str, Any]]]]:

        metrics: List[Dict[str, Any]] = [
            {
                "Namespace": metrics_config.namespace,
                "MetricName": metrics_config.metric_name,
                "Dimensions": [{"Name": dim.name, "Value": dim.value} for dim in query],
            }
            for query in metrics_config.metric_queries
        ]
        queries: List[Dict[str, Any]] = [
            {
                "Id": f"{str(metrics_config.metric_name).lower()}_query_{idx}",
                "MetricStat": {
                    "Metric": metric,
                    "Period": metrics_config.period,
                    "Stat": metrics_config.statistic,
                },
                "ReturnData": True,
            }
            for idx, metric in enumerate(metrics)
        ]

        customer_id_map = {}
        for query in queries:
            try:
                customer_id_map[query["Id"]] = next(
                    dim["Value"]
                    for dim in query["MetricStat"]["Metric"]["Dimensions"]
                    if dim["Name"] == metrics_config.customer_identifier
                )
            except StopIteration:
                logger.error(
                    f"Customer identifier {metrics_config.customer_identifier} not found in metric query dimensions for {metrics_config.metric_name}"
                )
                raise

        chunks = [
            queries[i : (i + self.metrics_query_chunk_size)]
            for i in range(0, len(queries), self.metrics_query_chunk_size)
        ]
        return customer_id_map, chunks

    @stamina.retry(on=Exception, attempts=5)
    def query_metrics(
        self, start_time: PositiveInt, end_time: PositiveInt
    ) -> Iterator[Dict[str, Any]]:
        """
        Fetch metrics from the specified AWS CloudWatch namespace.

        This method retrieves metrics and returns them as an iterator of dictionaries.
        It automatically retries up to 5 times if any exceptions occur during the metrics fetching process.

        Returns:
            Iterator[Dict[str, Any]]: An iterator that yields metric entries as dictionaries.

        Raises:
            Exception: If the metrics fetching operation fails after 5 attempts.
        """
        metrics_config = self.config.metrics_config

        if not metrics_config:
            msg = "Cloudwatch metrics configuration is missing."
            logger.error(msg, exc_info=True)
            raise CloudwatchClientError(msg)

        customer_ids, query_chunks = self._parse_metrics_queries(metrics_config)

        for queries in query_chunks:
            params = {
                "MetricDataQueries": queries,
                "StartTime": from_milliseconds(start_time),
                "EndTime": from_milliseconds(end_time),
                "MaxDatapoints": self.metrics_query_limit,
                "ScanBy": "TimestampAscending",
            }
            next_token = None

            while True:
                if next_token:
                    params["NextToken"] = next_token

                try:
                    response = self.metrics_client.get_metric_data(**params)
                    metric_data_results = response.get("MetricDataResults", {})

                    if not metric_data_results:
                        msg = f"No metric data results found for {metrics_config.metric_name}"
                        logger.error(msg, exc_info=True)
                        raise CloudwatchClientError(msg)

                    metrics = []

                    for metric_data in metric_data_results:
                        query_id = metric_data["Id"]
                        customer_id = customer_ids[query_id]
                        timestamps = metric_data["Timestamps"]
                        values = metric_data["Values"]

                        if len(timestamps) != len(values):
                            msg = f"Timestamps and values are not of the same length for {metrics_config.metric_name}"
                            logger.error(msg, exc_info=True)
                            raise CloudwatchClientError(msg)

                        metric_values = zip(timestamps, values)

                        for val in metric_values:
                            metrics.append(
                                {
                                    "customer_id": customer_id,
                                    "timestamp": val[0],
                                    "value": val[1],
                                    "label": metrics_config.metric_name,
                                }
                            )

                    logger.info(
                        f"Fetched {len(metrics)} metrics from Cloudwatch",
                        namespace=metrics_config.namespace,
                        metric_name=metrics_config.metric_name,
                        start_time=from_milliseconds(start_time).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                        end_time=from_milliseconds(end_time).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                    )

                    yield from metrics

                    if not response.get("NextToken"):
                        break

                except Exception as e:
                    msg = f"Failed to query metrics: {e}"
                    logger.error(msg, exc_info=True)
                    raise CloudwatchClientError(msg)


def get_cloudwatch_client():
    try:
        config = ConfigurationManager.load_config().input.cloudwatch
    except Exception as e:
        msg = f"Failed to load Cloudwatch configuration for client: {e}"
        logger.error(msg, exc_info=True)
        raise CloudwatchClientError(msg)
    return CloudwatchClient(config)
