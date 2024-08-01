from typing import Optional, Dict, Any

from pydantic import (
    BaseModel,
    PostgresDsn,
    model_validator,
    ConfigDict,
    StrictStr,
    StrictInt,
)
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session


class PostgresConfig(BaseModel):
    model_config = ConfigDict(validate_return=True)

    host: StrictStr
    port: StrictInt
    user: StrictStr
    database: StrictStr
    password: StrictStr

    dsn: StrictStr = ""

    @model_validator(mode="before")
    def assemble_api_db_connection(
        cls, values: Dict[StrictStr, Any]
    ) -> Dict[StrictStr, Any]:
        if not values.get("dsn"):
            values["dsn"] = PostgresDsn.build(
                scheme="postgresql",
                username=values.get("user"),
                password=values.get("password"),
                host=values.get("host"),
                port=values.get("port"),
                path=f"{values.get('database') or ''}",
            ).unicode_string()
        return values


class PostgresClient:
    """
    Reads and writes to postgres database
    """

    def __init__(self, config: PostgresConfig):
        self.engine = create_engine(config.dsn)
        self.sessionmaker = sessionmaker(bind=self.engine)
        self.session: Optional[Session] = None
        self.create_session()

    def create_session(self) -> None:
        if self.session is None:
            self.session = self.sessionmaker()

    def close(self) -> None:
        if self.session:
            self.session.close()
            self.session = None
