from enum import Enum
from typing import List, Optional

from pydantic import StrictStr, BaseModel, PositiveInt, model_validator


class DatadogStatistic(str, Enum):
    AVERAGE = "avg"
    MINIMUM = "min"
    MAXIMUM = "max"
    SUM = "sum"


class DatadogLogsConfig(BaseModel):
    indexes: List[StrictStr] = ["*"]
    query: StrictStr = "*"


class DatadogTag(BaseModel):
    customer_value: StrictStr
    tag_string: StrictStr


class DatadogMetricsConfig(BaseModel):
    metric_name: StrictStr
    interval: PositiveInt
    statistic: StrictStr
    organization_identifier: StrictStr
    metric_tag_data: List[DatadogTag]
    metric_queries: Optional[List[DatadogTag]] = None

    @model_validator(mode="before")
    def validate_statistic(cls, values):
        statistic = values.get("statistic")
        if statistic and statistic.upper() not in DatadogStatistic.__members__:
            raise ValueError(
                f'Invalid statistic found "{statistic}": please use one of {DatadogStatistic.__members__.keys()}'
                # noqa
            )
        return values

    @model_validator(mode="before")
    def convert_interval(cls, values):
        interval = values.get("interval")
        values["interval"] = interval * 1000
        return values

    @model_validator(mode="before")
    def validate_metric_tag_data(cls, values):
        metric_tag_data = values.get("metric_tag_data")
        organization_identifier = values.get("organization_identifier")
        for tag in metric_tag_data:
            customer_string = f"{organization_identifier}:{tag['customer_value']}"
            if customer_string not in tag["tag_string"]:
                raise ValueError(
                    "Customer identifier is not found in the metric tag string."
                )
        return values

    @model_validator(mode="after")
    def generate_metric_queries(self):
        metric_name = self.metric_name
        metric_statistic = self.statistic
        metric_value = DatadogStatistic[metric_statistic].value
        tag_data = self.metric_tag_data
        self.metric_queries = [
            DatadogTag(
                customer_value=tag.customer_value,
                tag_string=f"{metric_value}:{metric_name}{{{tag.tag_string}}}",
            )
            for tag in tag_data
        ]
        return self


class DatadogConfig(BaseModel):
    site: StrictStr
    api_key: StrictStr
    app_key: StrictStr
    logs_config: Optional[DatadogLogsConfig] = None
    metrics_config: Optional[DatadogMetricsConfig] = None
