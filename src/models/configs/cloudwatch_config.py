from typing import Optional

from pydantic import StrictStr, BaseModel, PositiveInt

from models.configs.indexer_config import get_indexer_config


class CloudwatchConfig(BaseModel):
    region_name: StrictStr
    aws_access_key_id: StrictStr
    aws_secret_access_key: StrictStr
    log_group: StrictStr
    start_time: PositiveInt
    filter_pattern: Optional[StrictStr] = None


def get_cloudwatch_config() -> CloudwatchConfig:
    config = get_indexer_config()
    return CloudwatchConfig(
        region_name=config.CLOUDWATCH_REGION,
        aws_access_key_id=config.CLOUDWATCH_AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.CLOUDWATCH_AWS_SECRET_ACCESS_KEY,
        log_group=config.CLOUDWATCH_LOG_GROUP,
        start_time=config.CLOUDWATCH_START_TIME,
        filter_pattern=config.CLOUDWATCH_FILTER_PATTERN,
    )
