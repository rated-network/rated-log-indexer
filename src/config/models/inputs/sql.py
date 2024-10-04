from pydantic import BaseModel, StrictStr, field_validator

from src.clients.postgres import PostgresConfig


class SqlConfig(BaseModel):
    sql: StrictStr
    metrics_config: PostgresConfig

    @field_validator("sql")
    def validate_sql_parameters(cls, v):
        required_params = [":start_time", ":end_time"]
        for param in required_params:
            if param not in v:
                raise ValueError(
                    f"SQL query must contain the parameter {param} in the query to be replaced"
                )
        return v

    @field_validator("sql")
    def validate_sql_fields(cls, v):
        required_fields = ["as timestamp", "as customer_id"]
        lower_sql = v.lower()
        for field in required_fields:
            if field not in lower_sql:
                raise ValueError(
                    f"SQL query must select the field '{field}'. Field needs to be aliased as '{field}'"
                )
        return v
