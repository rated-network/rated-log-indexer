from pydantic import BaseModel, root_validator, validator, StrictStr, StrictInt
from typing import Optional, Union
from datetime import datetime
from typing_extensions import Literal


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


class OffsetYamlConfig(BaseModel):
    type: Literal["postgres", "redis"]
    start_from: Union[StrictInt, datetime]
    start_from_type: Literal["bigint", "datetime"]

    postgres: Optional[OffsetPostgresYamlConfig] = None
    redis: Optional[OffsetRedisYamlConfig] = None

    @root_validator(pre=True)
    def validate_config_type(cls, values):
        offset_type = values.get("type")
        if offset_type == "postgres":
            if "postgres" not in values or not values["postgres"]:
                raise ValueError(
                    'postgres configuration is required when type is "postgres"'
                )
            values["redis"] = None
        elif offset_type == "redis":
            if "redis" not in values or not values["redis"]:
                raise ValueError('redis configuration is required when type is "redis"')
            values["postgres"] = None
        return values

    @root_validator(pre=True)
    def check_start_from_type_consistency(cls, values):
        start_from = values.get("start_from")
        start_from_type = values.get("start_from_type")

        if start_from_type == "bigint" and not isinstance(start_from, int):
            raise ValueError(
                "'start_from' must be an integer when 'start_from_type' is 'bigint'"
            )
        elif start_from_type == "datetime" and not isinstance(start_from, datetime):
            raise ValueError(
                "'start_from' must be a datetime object when 'start_from_type' is 'datetime'"
            )

        return values

    @validator("start_from")
    def validate_start_from(cls, v, values):
        start_from_type = values.get("start_from_type")
        if start_from_type == "bigint":
            if not isinstance(v, int):
                raise ValueError(f"Invalid 'start_from' value for bigint type: {v}")
        elif start_from_type == "datetime":
            if not isinstance(v, datetime):
                raise ValueError(f"Invalid 'start_from' value for datetime type: {v}")
        return v

    @validator("start_from_type")
    def validate_start_from_type(cls, v):
        if v not in ("bigint", "datetime", None):
            raise ValueError(f"Invalid 'start_from_type': {v}")
        return v
