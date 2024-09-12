from enum import Enum

from pydantic import (
    BaseModel,
    StrictStr,
    StrictInt,
    model_validator,
    field_validator,
    StrictBool,
)
from typing import Optional
from datetime import datetime


class OffsetPostgresYamlConfig(BaseModel):
    table_name: Optional[StrictStr] = "offset_tracking"
    host: StrictStr
    port: StrictInt
    database: StrictStr
    user: StrictStr
    password: StrictStr


class OffsetRedisYamlConfig(BaseModel):
    key: Optional[StrictStr] = "current_offset"
    host: StrictStr
    port: StrictInt
    db: StrictInt


class StartFromTypes(str, Enum):
    BIGINT = "bigint"
    DATETIME = "datetime"


class OffsetTypes(str, Enum):
    POSTGRES = "postgres"
    REDIS = "redis"


class OffsetYamlConfig(BaseModel):
    type: OffsetTypes
    override_start_from: StrictBool = False
    start_from: StrictInt
    start_from_type: StartFromTypes

    postgres: Optional[OffsetPostgresYamlConfig] = None
    redis: Optional[OffsetRedisYamlConfig] = None

    @model_validator(mode="before")
    def validate_config_type(cls, values):
        offset_type = values.get("type")
        if offset_type == OffsetTypes.POSTGRES:
            if "postgres" not in values or not values["postgres"]:
                raise ValueError(
                    'postgres configuration is required when type is "postgres"'
                )
            values["redis"] = None
        elif offset_type == OffsetTypes.REDIS:
            if "redis" not in values or not values["redis"]:
                raise ValueError('redis configuration is required when type is "redis"')
            values["postgres"] = None
        return values

    @model_validator(mode="before")
    def check_start_from_type_consistency(cls, values):
        start_from = values.get("start_from")
        start_from_type = values.get("start_from_type")

        if start_from_type == StartFromTypes.BIGINT and not isinstance(start_from, int):
            raise ValueError(
                "'start_from' must be an integer when 'start_from_type' is 'bigint'"
            )
        elif start_from_type == StartFromTypes.DATETIME and not isinstance(
            start_from, datetime
        ):
            raise ValueError(
                "'start_from' must be a datetime object when 'start_from_type' is 'datetime'"
            )

        return values

    @field_validator("start_from")
    def validate_start_from(cls, v, info):
        start_from_type = info.data.get("start_from_type")
        if start_from_type == StartFromTypes.BIGINT:
            if not isinstance(v, int):
                raise ValueError(f"Invalid 'start_from' value for bigint type: {v}")
        elif start_from_type == StartFromTypes.DATETIME:
            if not isinstance(v, datetime):
                raise ValueError(f"Invalid 'start_from' value for datetime type: {v}")
        return v

    @field_validator("start_from_type")
    def validate_start_from_type(cls, v):
        if v not in StartFromTypes:
            raise ValueError(f"Invalid 'start_from_type': {v}")
        return v
