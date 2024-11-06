from rated_parser.payloads.log_patterns import (  # type: ignore
    JsonFieldDefinition,
    RawTextFieldDefinition,
    LogFormat as RatedParserLogFormat,
)
from rated_parser.payloads.metric_patterns import MetricFieldDefinition  # type: ignore
from pydantic import BaseModel, StrictStr, StrictInt, field_validator, model_validator
from typing import List, Union, Dict


class BaseFilterConfig(BaseModel):
    version: StrictInt


class LogFilterConfig(BaseFilterConfig):
    log_format: RatedParserLogFormat
    fields: List[Union[JsonFieldDefinition, RawTextFieldDefinition]]
    log_example: Union[StrictStr, Dict]

    @field_validator("log_format", mode="before")
    def normalize_log_format(cls, v):
        if isinstance(v, str):
            return RatedParserLogFormat(v.lower())
        return v

    @model_validator(mode="after")
    def validate_config(self):
        # Validate fields
        if not self.fields:
            raise ValueError("Filter fields cannot be empty")

        if not any(field.key == "organization_id" for field in self.fields):
            raise ValueError("organization_id field is required in filters")

        # Validate log example format matches log_format
        if self.log_format == RatedParserLogFormat.RAW_TEXT and not isinstance(
            self.log_example, str
        ):
            raise ValueError("log_example must be a string when log_format is RAW_TEXT")
        if self.log_format == RatedParserLogFormat.JSON and not isinstance(
            self.log_example, dict
        ):
            raise ValueError("log_example must be a dictionary when log_format is JSON")

        # Validate field types match log_format
        expected_type = (
            RawTextFieldDefinition
            if self.log_format == RatedParserLogFormat.RAW_TEXT
            else JsonFieldDefinition
        )

        for field in self.fields:
            if not isinstance(field, expected_type):
                raise ValueError(
                    f"All fields must be {expected_type.__name__} when log_format is {self.log_format.value}"
                )

        return self


class MetricFilterConfig(BaseFilterConfig):
    fields: List[MetricFieldDefinition]

    @model_validator(mode="after")
    def validate_config(self):
        if not self.fields:
            raise ValueError("Filter fields cannot be empty")

        return self
