from enum import Enum
from typing import Optional, List

from pydantic import BaseModel, StrictStr, PositiveInt, model_validator


class CloudwatchStatistic(str, Enum):
    # These are a sample of the supported statistic types for Cloudwatch metrics, and can be expanded as needed.
    # However, the others are more configurable, and require percentile ranges.
    # https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/Statistics-definitions.html
    AVERAGE = "Average"
    MINIMUM = "Minimum"
    MAXIMUM = "Maximum"
    SUM = "Sum"
    SAMPLE_COUNT = "SampleCount"


class CloudwatchDimension(BaseModel):
    name: StrictStr
    value: StrictStr


class CloudwatchLogsConfig(BaseModel):
    log_group_name: StrictStr
    log_stream_name: Optional[StrictStr] = None
    filter_pattern: Optional[StrictStr] = None


class CloudwatchMetricsConfig(BaseModel):
    namespace: StrictStr
    metric_name: StrictStr
    period: PositiveInt
    statistic: StrictStr
    customer_identifier: StrictStr
    metric_queries: List[List[CloudwatchDimension]]

    @model_validator(mode="before")
    def validate_statistic(cls, values):
        statistic = values.get("statistic")
        if statistic and statistic.upper() not in CloudwatchStatistic.__members__:
            raise ValueError(
                f'Invalid statistic found "{statistic}": please use one of {CloudwatchStatistic.__members__.keys()}'
                # noqa
            )
        values["statistic"] = CloudwatchStatistic[statistic].value
        return values

    @model_validator(mode="before")
    def validate_metric_queries(cls, values):
        metric_queries = values.get("metric_queries")
        customer_identifier = values.get("customer_identifier")
        if metric_queries:
            for query in metric_queries:
                if customer_identifier not in [q.name for q in query]:
                    raise ValueError(
                        "Customer identifier is not found in the metric query dimensions."
                    )
        return values


class CloudwatchConfig(BaseModel):
    region: StrictStr
    aws_access_key_id: StrictStr
    aws_secret_access_key: StrictStr
    logs_config: Optional[CloudwatchLogsConfig] = None
    metrics_config: Optional[CloudwatchMetricsConfig] = None
