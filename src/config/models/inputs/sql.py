from pydantic import BaseModel, StrictStr

from src.clients.postgres import PostgresConfig


class SqlConfig(BaseModel):
    sql: StrictStr
    metrics_config: PostgresConfig
