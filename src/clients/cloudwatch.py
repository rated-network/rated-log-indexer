from enum import Enum
from typing import Any, Dict, Iterator, Optional, List, Tuple

import stamina
import structlog
from boto3 import client  # type: ignore
from botocore.client import BaseClient  # type: ignore
from botocore.config import Config  # type: ignore
from botocore.exceptions import ClientError  # type: ignore
from pydantic import PositiveInt

from src.config.models.inputs.cloudwatch import (
    CloudwatchConfig,
    CloudwatchMetricsConfig,
)
from src.utils.time_conversion import from_milliseconds

logger = structlog.get_logger(__name__)


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


class CloudwatchInputs(str, Enum):
    LOGS = "logs"
    METRICS = "metrics"


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

    @stamina.retry(on=CloudwatchClientError)
    def make_api_call(
        self, call_type: CloudwatchInputs, params: Dict[str, Any]
    ) -> Dict[str, Any]:
        try:
            if call_type == CloudwatchInputs.LOGS:
                response = self.logs_client.filter_log_events(**params)
                return response
            elif call_type == CloudwatchInputs.METRICS:
                response = self.metrics_client.get_metric_data(**params)
                return response
            else:
                msg = f"Unsupported call type: {call_type}"
                logger.error(msg, exc_info=True)
                raise CloudwatchClientError(msg)
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            if error_code == "ThrottlingException":
                msg = "Rate limit hit, retrying"
                logger.warning(msg, exc_info=True)
                raise CloudwatchClientError(msg) from e
            else:
                logger.error(
                    f"Client error querying CloudWatch: {error_code}",
                    exc_info=True,
                )
                raise CloudwatchClientError(
                    f"Failed to query CloudWatch with ClientError: {error_code}"
                ) from e
        except Exception as e:
            msg = "Unexpected error querying CloudWatch"
            logger.error(msg, exc_info=True)
            raise CloudwatchClientError(msg) from e

    def query_logs(
        self,
        start_time: PositiveInt,
        end_time: PositiveInt,
    ) -> Iterator[Dict[str, Any]]:
        logs_config = self.config.logs_config

        if not logs_config:
            msg = "Cloudwatch logs configuration is missing."
            logger.error(msg, exc_info=True)
            raise CloudwatchClientError(msg)

        params = {
            "logGroupName": logs_config.log_group_name,
            "startTime": start_time,
            "endTime": end_time,
            "limit": self.logs_query_limit,
        }

        if logs_config.filter_pattern:
            params["filterPattern"] = logs_config.filter_pattern

        next_token = None
        total_logs = 0
        page_count = 0

        while True:
            if next_token:
                params["nextToken"] = next_token

            try:
                events_batch = self.make_api_call(CloudwatchInputs.LOGS, params)
                logs = events_batch.get("events", [])
                batch_count = len(logs)
                total_logs += batch_count
                page_count += 1

                logger.debug(
                    f"Fetched page {page_count}: {batch_count} logs (Total: {total_logs})",
                    start_time=start_time,
                    end_time=end_time,
                    log_group_name=logs_config.log_group_name,
                )

                yield from logs

                next_token = events_batch.get("nextToken")
                if not next_token:
                    break
            except CloudwatchClientError as e:
                msg = f"Failed to query logs for {logs_config.log_group_name} on page {page_count}"
                logger.error(msg, exc_info=True)
                raise CloudwatchClientError(msg) from e

        logger.info(
            f"Fetched logs {total_logs} from Cloudwatch",
            page_count=page_count,
            start_time=start_time,
            end_time=end_time,
            log_group_name=logs_config.log_group_name,
            start_time_str=from_milliseconds(start_time).strftime("%Y-%m-%d %H:%M:%S"),
            end_time_str=from_milliseconds(end_time).strftime("%Y-%m-%d %H:%M:%S"),
        )

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

        organization_id_map = {}
        for query in queries:
            try:
                organization_id_map[query["Id"]] = next(
                    dim["Value"]
                    for dim in query["MetricStat"]["Metric"]["Dimensions"]
                    if dim["Name"] == metrics_config.organization_identifier
                )
            except StopIteration:
                logger.error(
                    f"Customer identifier {metrics_config.organization_identifier} not found in metric query dimensions for {metrics_config.metric_name}"
                )
                raise

        chunks = [
            queries[i : (i + self.metrics_query_chunk_size)]
            for i in range(0, len(queries), self.metrics_query_chunk_size)
        ]
        return organization_id_map, chunks

    def query_metrics(
        self, start_time: PositiveInt, end_time: PositiveInt
    ) -> Iterator[Dict[str, Any]]:
        metrics_config = self.config.metrics_config

        if not metrics_config:
            msg = "Cloudwatch metrics configuration is missing."
            logger.error(msg, exc_info=True)
            raise CloudwatchClientError(msg)

        organization_ids, query_chunks = self._parse_metrics_queries(metrics_config)

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
                    response = self.make_api_call(CloudwatchInputs.METRICS, params)
                    metric_data_results = response.get("MetricDataResults", {})

                    if not metric_data_results:
                        msg = f"No metric data results found for {metrics_config.metric_name}"
                        logger.error(msg, exc_info=True)
                        raise CloudwatchClientError(msg)

                    metrics = []

                    for metric_data in metric_data_results:
                        query_id = metric_data["Id"]
                        organization_id = organization_ids[query_id]
                        timestamps = metric_data["Timestamps"]
                        values = metric_data["Values"]

                        if len(timestamps) != len(values):
                            msg = f"Timestamps and values are not of the same length for {metrics_config.metric_name}"
                            logger.error(msg)
                            raise CloudwatchClientError(msg)

                        metric_values = zip(timestamps, values)

                        for val in metric_values:
                            metrics.append(
                                {
                                    "organization_id": organization_id,
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

                except CloudwatchClientError as e:
                    msg = "Failed to query Cloudwatch metrics"
                    logger.error(msg, exc_info=True)
                    raise CloudwatchClientError(msg) from e

    def query_objects(
        self, start_time: PositiveInt, end_time: PositiveInt
    ) -> Iterator[Dict[str, Any]]:
        raise NotImplementedError("Querying objects is not supported for Cloudwatch")
