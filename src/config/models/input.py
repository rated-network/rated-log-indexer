from enum import Enum

from pydantic import BaseModel, StrictStr, model_validator, PositiveInt
from typing import Optional, List


class IntegrationTypes(str, Enum):
    CLOUDWATCH = "cloudwatch"
    DATADOG = "datadog"


class InputTypes(str, Enum):
    LOGS = "logs"
    METRICS = "metrics"


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


class DatadogLogsConfig(BaseModel):
    indexes: List[StrictStr] = ["*"]
    query: StrictStr = "*"


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


class DatadogConfig(BaseModel):
    site: StrictStr
    api_key: StrictStr
    app_key: StrictStr
    logs_config: Optional[DatadogLogsConfig] = None


class InputYamlConfig(BaseModel):
    integration: IntegrationTypes
    type: InputTypes
    cloudwatch: Optional[CloudwatchConfig] = None
    datadog: Optional[DatadogConfig] = None

    @model_validator(mode="before")
    def validate_input_config(cls, values):
        integration_type = values.get("integration")
        if integration_type:
            config_attr = integration_type
            if not values.get(config_attr):
                raise ValueError(
                    f'Configuration for input source "{integration_type}" is not found. Please add input configuration for {integration_type}.'
                    # noqa
                )
            for key in IntegrationTypes:
                if key != config_attr:
                    values[key.value] = None
        return values

    @model_validator(mode="before")
    def validate_integration_source(cls, values):
        integration_type = values.get("integration")
        if (
            integration_type
            and integration_type.upper() not in IntegrationTypes.__members__
        ):
            raise ValueError(
                f'Invalid input source found "{integration_type}": please use one of {IntegrationTypes.__members__.keys()}'  # noqa
            )
        return values

    @model_validator(mode="before")
    def validate_input_type(cls, values):
        input_type = values.get("type")
        if input_type and input_type.upper() not in InputTypes.__members__:
            raise ValueError(
                f'Invalid input source found "{input_type}": please use one of {InputTypes.__members__.keys()}'
                # noqa
            )
        return values
