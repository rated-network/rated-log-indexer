from rated_parser import LogFormat as RatedParserLogFormat  # type: ignore
from rated_parser.core.payloads import FieldType as RatedParserFieldType  # type: ignore
from pydantic import BaseModel, StrictStr, StrictInt, field_validator
from typing import List, Optional, Union, Dict


class FieldConfig(BaseModel):
    key: StrictStr
    value: StrictStr
    field_type: RatedParserFieldType
    format: Optional[StrictStr] = None

    @field_validator("format")
    def validate_timestamp_format(cls, v, info):
        field_type = info.data.get("field_type")
        if field_type == RatedParserFieldType.TIMESTAMP:
            if v is None:
                raise ValueError("Format must not be null for TIMESTAMP field type")
        return v


class FiltersYamlConfig(BaseModel):
    version: StrictInt
    log_format: RatedParserLogFormat = RatedParserLogFormat.RAW_TEXT
    log_example: Union[StrictStr, Dict]
    fields: List[FieldConfig]

    @field_validator("log_example")
    def validate_log_example(cls, v, info):
        log_format = info.data.get("log_format")

        if log_format == RatedParserLogFormat.RAW_TEXT and not isinstance(v, str):
            raise ValueError("log_example must be a string when log_format is RAW_TEXT")
        elif log_format == RatedParserLogFormat.JSON and not isinstance(v, dict):
            raise ValueError("log_example must be a dictionary when log_format is JSON")
        return v
