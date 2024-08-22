from typing import Any, Iterator, Dict

import stamina
import structlog
from datadog_api_client.v2.model.logs_list_request import LogsListRequest
from datadog_api_client.v2.model.logs_list_request_page import LogsListRequestPage
from datadog_api_client.v2.model.logs_query_filter import LogsQueryFilter
from datadog_api_client.v2.model.logs_sort import LogsSort
from pydantic import PositiveInt

from src.config.manager import ConfigurationManager
from src.config.models.input import DatadogConfig
from datadog_api_client import ApiClient, Configuration
from datadog_api_client.v2.api.logs_api import LogsApi

from src.utils.time_conversion import from_milliseconds


logger = structlog.get_logger("datadog_client")


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
    ) -> Iterator[Dict[str, Any]]:
        logs_config = self.config.logs_config

        if not logs_config:
            logger.error("Datadog logs configuration is missing.", exc_info=True)
            raise

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
                response = self.logs_api.list_logs(body=request_body)
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
                logger.error(f"Failed to query logs: {e}")
                raise e


def get_datadog_client():
    try:
        config = ConfigurationManager.load_config().input.datadog
    except Exception as e:
        logger.error(f"Failed to load Cloudwatch configuration for client: {e}")
        raise e
    return DatadogClient(config)
