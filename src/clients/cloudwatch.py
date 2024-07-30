from typing import List, Any, Dict, Union

from boto3 import client  # type: ignore
from botocore.config import Config  # type: ignore
from pydantic import PositiveInt, StrictStr

from models.configs.cloudwatch_config import CloudwatchConfig, get_cloudwatch_config
from utils.logger import logger


class CloudwatchClient:
    def __init__(self, config: CloudwatchConfig, limit: PositiveInt = 10_000):
        self.config = config
        self.client = client(
            "logs",
            config=Config(
                region_name=config.region_name,
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
    ) -> List[Union[Dict[str, Any], StrictStr]]:
        logs = []
        filter_pattern = self.config.filter_pattern
        params = {
            "logGroupName": self.config.log_group,
            "startTime": start_time,
            "endTime": end_time,
            "limit": self.limit,
        }

        if filter_pattern:
            params["filterPattern"] = filter_pattern

        try:
            events_batch = self.client.filter_log_events(**params)
            logs.extend(events_batch["events"])
            logger.info(f"Fetched {len(logs)} logs from {self.config.log_group}")
        except Exception as e:
            logger.error(f"Failed first query for logs: {e}")
            raise e

        while len(events_batch["events"]) == self.limit:
            next_token = events_batch.get("nextToken")
            if next_token:
                try:
                    events_batch = self.client.filter_log_events(
                        **params, nextToken=next_token
                    )
                    logger.info(
                        f"Fetched {len(events_batch['events'])} logs from {self.config.log_group}"
                    )
                    logs.extend(events_batch["events"])
                    logger.info(f"Total logs fetched: {len(logs)}")
                except Exception as e:
                    logger.error(f"Failed to query logs: {e}")
                    raise e
            else:
                break

        return logs


def cloudwatch_client() -> CloudwatchClient:
    config = get_cloudwatch_config()
    return CloudwatchClient(config)
