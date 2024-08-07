from typing import Any, Dict, Union, Iterator

from boto3 import client  # type: ignore
from botocore.config import Config  # type: ignore
from pydantic import PositiveInt, StrictStr

from src.config.manager import ConfigurationManager
from src.config.models.input import CloudwatchConfig
from src.utils.logger import logger


class CloudwatchClient:
    def __init__(self, config: CloudwatchConfig, limit: PositiveInt = 10_000):
        self.config = config
        self.logs_client = client(
            "logs",
            config=Config(
                region_name=config.region,
                retries={"max_attempts": 10, "mode": "standard"},
            ),
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
        )
        self.limit = (
            10_000 if limit > 10_000 else limit
        )  # Cannot be greater than 10_000 as per AWS documentation.

    def query_logs(
        self,
        start_time: PositiveInt,
        end_time: PositiveInt,
    ) -> Iterator[Union[Dict[str, Any], StrictStr]]:

        filter_pattern = self.config.filter_pattern
        params = {
            "logGroupName": self.config.log_group_name,
            "startTime": start_time,
            "endTime": end_time,
            "limit": self.limit,
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
                    f"Fetched {len(logs)} logs from {self.config.log_group_name}"
                )
                yield from logs

                if len(logs) < self.limit:
                    break

                next_token = events_batch.get("nextToken")
                if not next_token:
                    break
            except Exception as e:
                logger.error(f"Failed to query logs: {e}")
                raise e


def get_cloudwatch_client():
    try:
        config = ConfigurationManager.load_config().input.cloudwatch
    except Exception as e:
        logger.error(f"Failed to load Cloudwatch configuration for client: {e}")
        raise e
    return CloudwatchClient(config)
