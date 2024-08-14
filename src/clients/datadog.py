from typing import Any, Iterator

import stamina
from datadog_api_client.v2.model.logs_list_request import LogsListRequest
from datadog_api_client.v2.model.logs_list_request_page import LogsListRequestPage
from datadog_api_client.v2.model.logs_query_filter import LogsQueryFilter
from datadog_api_client.v2.model.logs_sort import LogsSort
from pydantic import PositiveInt

from src.config.models.input import DatadogConfig
from datadog_api_client import ApiClient, Configuration
from datadog_api_client.v2.api.logs_api import LogsApi

from src.utils.logger import logger
from src.utils.time_conversion import from_milliseconds

PAGE_LIMIT = 1000
SORT_METHOD = LogsSort.TIMESTAMP_ASCENDING


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

    @stamina.retry(on=Exception, attempts=5)
    def query_logs(
        self, start_time: PositiveInt, end_time: PositiveInt
    ) -> Iterator[Any]:
        logs_config = self.config.logs_config

        if not logs_config:
            logger.error("Datadog logs configuration is missing.", exc_info=True)
            raise

        start_date = from_milliseconds(start_time)
        end_date = from_milliseconds(end_time)

        filter_query = LogsQueryFilter(
            indexes=logs_config.indexes,
            _from=start_date.isoformat(),  # 2020-09-17T12:48:36+01:00
            to=end_date.isoformat(),  # 2020-09-17T12:58:36+01:00
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
                response = self.logs_api.list_logs(body=request_body)
                data = response.get("data", [])

                yield from data

                cursor = response["meta"].get("page", {}).get("after")
                if not cursor:
                    break
            except Exception as e:
                logger.error(f"Failed to query logs: {e}")
                raise e
