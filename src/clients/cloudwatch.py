from typing import List, Any

from boto3 import client  # type: ignore
from botocore.config import Config  # type: ignore
from pydantic import PositiveInt, StrictStr

from src.configs.cloudwatch_config import CloudwatchConfig
from src.utils.logger import logger


class CloudwatchClient:
    def __init__(self, config: CloudwatchConfig):
        self.config = config
        self.client = client(
            "logs",
            config=Config(
                region_name=config.region_name,
                retries={
                    'max_attempts': 10,
                    'mode': 'standard'
                }
            ),
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
        )
        self.limit = 100

    def query_logs(self, start_time: PositiveInt, end_time: PositiveInt, filter_pattern: StrictStr) -> List[Any]:
        logs = []

        try:
            events_batch = self.client.filter_log_events(
                logGroupName=self.config.log_group,
                startTime=start_time,
                endTime=end_time,
                filterPattern=filter_pattern,
                limit=self.limit
            )
            logs.extend(events_batch["events"])
            logger.info(f"Fetched {len(logs)} logs from {self.config.log_group}")
        except Exception as e:
            logger.error(f"Failed first query for logs: {e}")
            raise e

        while len(events_batch["events"]) == self.limit:
            next_token = events_batch.get("nextToken")
            logger.info(f"Next token: {next_token}")
            if next_token:
                try:
                    events_batch = self.client.filter_log_events(
                        logGroupName=self.config.log_group,
                        startTime=start_time,
                        endTime=end_time,
                        filterPattern=filter_pattern,
                        limit=self.limit,
                        nextToken=next_token
                    )
                    logger.info(f"Fetched {len(events_batch['events'])} logs from {self.config.log_group}")
                    logs.extend(events_batch["events"])
                    logger.info(f"Total logs fetched: {len(logs)}")
                except Exception as e:
                    logger.error(f"Failed to query logs: {e}")
                    raise e
            else:
                break

        return logs
