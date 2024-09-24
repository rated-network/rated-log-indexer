from enum import Enum
from time import sleep
from typing import Any, Iterator, Dict, List, Union

import stamina
import structlog
from datadog_api_client.exceptions import ApiException
from datadog_api_client.v2.api.metrics_api import MetricsApi
from datadog_api_client.v2.model.logs_list_response import LogsListResponse
from datadog_api_client.v2.model.metrics_data_source import MetricsDataSource
from datadog_api_client.v2.model.metrics_timeseries_query import MetricsTimeseriesQuery
from datadog_api_client.v2.model.timeseries_formula_query_request import (
    TimeseriesFormulaQueryRequest,
)
from datadog_api_client.v2.model.timeseries_formula_request import (
    TimeseriesFormulaRequest,
)
from datadog_api_client.v2.model.timeseries_formula_request_attributes import (
    TimeseriesFormulaRequestAttributes,
)
from datadog_api_client.v2.model.timeseries_formula_request_queries import (
    TimeseriesFormulaRequestQueries,
)
from datadog_api_client.v2.model.timeseries_formula_request_type import (
    TimeseriesFormulaRequestType,
)
from datadog_api_client.v2.model.logs_list_request import LogsListRequest
from datadog_api_client.v2.model.logs_list_request_page import LogsListRequestPage
from datadog_api_client.v2.model.logs_query_filter import LogsQueryFilter
from datadog_api_client.v2.model.logs_sort import LogsSort
from pydantic import PositiveInt

from src.config.models.inputs.datadog import DatadogConfig
from datadog_api_client import ApiClient, Configuration
from datadog_api_client.v2.api.logs_api import LogsApi

from src.utils.time_conversion import from_milliseconds

PAGE_LIMIT = 1000
SORT_METHOD = LogsSort.TIMESTAMP_ASCENDING
DATADOG_EPOCH_LIMIT = 20_000


logger = structlog.get_logger(__name__)


class DatadogInputs(str, Enum):
    LOGS = "logs"
    METRICS = "metrics"


class DatadogClientError(Exception):
    """Custom exception for Cloudwatch Client errors."""

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class DatadogClient:
    def __init__(self, config: DatadogConfig):
        self.config = config
        self.datadog_config = Configuration(
            host=self.config.site,
            api_key={
                "apiKeyAuth": self.config.api_key,
                "appKeyAuth": self.config.app_key,
            },
        )
        self.client = ApiClient(self.datadog_config)
        self.logs_api = LogsApi(self.client)
        self.metrics_api = MetricsApi(self.client)

        self.datadog_config.unstable_operations["query_timeseries_data"] = True

    @stamina.retry(on=DatadogClientError)
    def make_api_call(
        self,
        call_type: DatadogInputs,
        request_body: Union[LogsListRequest, TimeseriesFormulaQueryRequest],
    ) -> Union[LogsListResponse, Dict[str, Any]]:
        try:
            if call_type == DatadogInputs.LOGS and isinstance(
                request_body, LogsListRequest
            ):
                response = self.logs_api.list_logs(body=request_body)
                return response
            elif call_type == DatadogInputs.METRICS and isinstance(
                request_body, TimeseriesFormulaQueryRequest
            ):
                response = self.metrics_api.query_timeseries_data(
                    request_body
                ).to_dict()
                return response
            else:
                msg = f"Unsupported call type: {call_type.value}"
                logger.error(msg, exc_info=True)
                raise DatadogClientError(msg)
        except ApiException as e:
            if e.status == 429:
                retry_after = int(e.headers.get("x-ratelimit-reset", 5))
                logger.warning(
                    f"Rate limit hit, retrying after {retry_after} seconds.",
                    exc_info=True,
                )
                sleep(retry_after)
                raise DatadogClientError("Rate limit hit, retrying") from e
            else:
                logger.error(
                    f"Unexpected ApiException querying Datadog for {call_type.value}",
                    exc_info=True,
                )
                raise DatadogClientError(f"Failed to query {call_type.value}") from e
        except Exception as e:
            logger.error(
                f"Unexpected error querying Datadog for {call_type.value}",
                exc_info=True,
            )
            raise DatadogClientError(f"Failed to query {call_type.value}") from e

    def query_logs(
        self, start_time: PositiveInt, end_time: PositiveInt
    ) -> Iterator[Dict[str, Any]]:
        logs_config = self.config.logs_config

        if not logs_config:
            msg = "Datadog logs configuration is missing."
            logger.error(msg)
            raise DatadogClientError(msg)

        filter_query = LogsQueryFilter(
            indexes=logs_config.indexes,
            _from=str(start_time),
            to=str(end_time),
            query=logs_config.query,
        )
        request_page = LogsListRequestPage(
            limit=PAGE_LIMIT,
        )
        cursor = None

        while True:
            if cursor:
                request_page.cursor = cursor

            request_body = LogsListRequest(
                filter=filter_query,
                sort=SORT_METHOD,
                page=request_page,
            )

            try:
                response = self.make_api_call(DatadogInputs.LOGS, request_body)

                logs = response.get("data", [])
                data = [log.to_dict() for log in logs]

                logger.info(
                    f"Fetched {len(data)} logs from Datadog",
                    start_time=start_time,
                    end_time=end_time,
                    indexes=logs_config.indexes,
                    start_time_str=from_milliseconds(start_time).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                    end_time_str=from_milliseconds(end_time).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                )

                yield from data

                cursor = response["meta"].get("page", {}).get("after")
                if not cursor:
                    break

            except Exception as e:
                msg = "Failed to query logs"
                logger.error(msg, exc_info=True)
                raise DatadogClientError(msg) from e

    def _parse_metrics_response(self, response: Dict[str, Any]) -> List[Dict[str, Any]]:
        metrics_config = self.config.metrics_config

        if not metrics_config:
            msg = "Datadog metrics configuration is missing."
            logger.error(msg)
            raise DatadogClientError(msg)

        data = response.get("data", {}).get("attributes", {})
        series = data.get("series", [])
        timestamps = data.get("times", [])
        values = data.get("values", [])
        query_indices = [s["query_index"] for s in series]
        metrics_data = []

        for v in values:
            try:
                value_list = v.__dict__["_data_store"]["value"]
                metrics_data.append(list(zip(timestamps, value_list)))
            except Exception:
                msg = "Failed to parse metrics response"
                logger.error(msg, exc_info=True)
                raise DatadogClientError(msg)

        if not metrics_config.metric_queries:
            msg = f"Datadog metrics queries missing for {metrics_config.metric_name}"
            logger.error(msg)
            raise DatadogClientError(msg)

        metrics_values = [
            {
                "metric_name": metrics_config.metric_name,
                "customer_id": metrics_config.metric_queries[i].customer_value,
                "query_index": i,
                "data": [
                    {"timestamp": timestamp, "value": value}
                    for timestamp, value in metrics_data[idx]
                    if value
                ],
            }
            for idx, i in enumerate(query_indices)
        ]

        return metrics_values

    def query_metrics(
        self, start_time: PositiveInt, end_time: PositiveInt
    ) -> Iterator[Dict[str, Any]]:
        metrics_config = self.config.metrics_config

        if not metrics_config or not metrics_config.metric_queries:
            msg = "Datadog metrics configuration or queries are missing."
            logger.error(msg)
            raise DatadogClientError(msg)

        queries = []

        for metric_query in metrics_config.metric_queries:
            query = MetricsTimeseriesQuery(
                data_source=MetricsDataSource.METRICS,
                query=metric_query.tag_string,
                name=metric_query.customer_value,
            )
            queries.append(query)

        formula_request = TimeseriesFormulaRequest(
            attributes=TimeseriesFormulaRequestAttributes(
                _from=start_time,
                to=end_time,
                queries=TimeseriesFormulaRequestQueries(queries),
                interval=metrics_config.interval,
            ),
            type=TimeseriesFormulaRequestType.TIMESERIES_REQUEST,
        )

        request = TimeseriesFormulaQueryRequest(
            data=formula_request,
        )

        try:
            response = self.make_api_call(DatadogInputs.METRICS, request)

            if isinstance(response, dict):
                data = self._parse_metrics_response(response)
            else:
                msg = "Failed to parse metrics response"
                logger.error(msg, exc_info=True)
                raise DatadogClientError(msg)

            flattened_data = [
                {
                    "metric_name": d["metric_name"],
                    "customer_id": d["customer_id"],
                    "timestamp": item["timestamp"],
                    "value": item["value"],
                }
                for d in data
                for item in d["data"]
            ]

            logger.info(
                f"Fetched {len(flattened_data)} metrics from Datadog",
                start_time=start_time,
                end_time=end_time,
                metric_name=metrics_config.metric_name,
                start_time_str=from_milliseconds(start_time).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                end_time_str=from_milliseconds(end_time).strftime("%Y-%m-%d %H:%M:%S"),
            )

            yield from flattened_data
        except Exception as e:
            msg = "Failed to query Datadog metrics"
            logger.error(msg, exc_info=True)
            raise DatadogClientError(msg) from e
