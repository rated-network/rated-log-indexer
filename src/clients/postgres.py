from datetime import datetime, timezone
from typing import Optional, Dict, Any, Iterator

import structlog
from pydantic import (
    BaseModel,
    PostgresDsn,
    model_validator,
    ConfigDict,
    StrictStr,
    StrictInt,
    PositiveInt,
)
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.sql import sqltypes
from sqlalchemy.engine.reflection import Inspector

logger = structlog.get_logger(__name__)


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


class SQLClientError(Exception):
    """Custom exception for SQL Client errors."""

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class PostgresClient:
    """
    Reads from postgres database and formats data for slaOS integration
    """

    def __init__(
        self, config: PostgresConfig, integration_query: Optional[StrictStr] = None
    ):
        self.config = config
        self.engine = create_engine(config.dsn)
        self.sessionmaker = sessionmaker(bind=self.engine)
        self.session: Optional[Session] = None
        self.create_session()

        """ This bit of code is used to run PostresClient as an integration source. """
        self.integration_query = integration_query
        if self.integration_query:
            self._validate_query()

    def create_session(self) -> None:
        if self.session is None:
            self.session = self.sessionmaker()

    def close(self) -> None:
        if self.session:
            self.session.close()
            self.session = None

    def _validate_query(self) -> None:
        """
        Validates the query to ensure it meets the necessary requirements:
        1. Compiles the query to check for syntax errors
        2. Checks for required columns: timestamp and customer_id
        3. Ensures the query returns additional columns for values
        4. Checks the timestamp type
        """
        try:
            # Compile the query using the session
            query = text(self.integration_query)  # type: ignore
            compiled_query = query.compile(
                self.session.bind, compile_kwargs={"literal_binds": True}  # type: ignore
            )

            # Get the column information
            result = self.session.execute(  # type: ignore
                text(
                    "SELECT * FROM (" + self.integration_query + ") AS subquery LIMIT 0"  # type: ignore
                )
            )

            columns = result.keys()

            if not columns:
                raise SQLClientError("Unable to determine query structure")

            # Check for required columns
            if "timestamp" not in columns or "customer_id" not in columns:
                raise SQLClientError(
                    "Query result must include 'timestamp' and 'customer_id' columns"
                )

            # Check if there are additional columns for values
            if len(columns) <= 2:
                raise SQLClientError(
                    "Query should return additional columns for values besides timestamp and customer_id"
                )

            # Check timestamp type
            inspector: Inspector = inspect(self.session.bind)  # type: ignore
            column_info = inspector.get_columns(compiled_query.statement)  # type: ignore
            timestamp_column = next(
                col for col in column_info if col["name"] == "timestamp"
            )
            timestamp_type = timestamp_column["type"]

            valid_types = (
                sqltypes.DateTime,
                sqltypes.TIMESTAMP,
                sqltypes.Integer,
                sqltypes.BigInteger,
                sqltypes.Float,
                sqltypes.Numeric,
            )

            if not isinstance(timestamp_type, valid_types):
                raise SQLClientError(
                    "Timestamp must be a DateTime, Integer, or Float type"
                )

            logger.info("Query validation successful")

        except SQLAlchemyError as e:
            if "relation" in str(e) and "does not exist" in str(e):
                raise SQLClientError(f"Relation does not exist: {str(e)}") from e
            elif "syntax error" in str(e).lower():
                raise SQLClientError(f"SQL syntax error: {str(e)}") from e
            else:
                raise SQLClientError(f"Error validating query: {str(e)}") from e
        finally:
            self.close()

    def _format_timestamp(self, timestamp: Any) -> str:
        if isinstance(timestamp, datetime):
            return (
                timestamp.replace(tzinfo=timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
            )
        elif isinstance(timestamp, (int, float)):
            return (
                datetime.fromtimestamp(timestamp, tz=timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
            )
        else:
            raise SQLClientError(f"Unsupported timestamp format: {type(timestamp)}")

    def query_metrics(self, start_time: int, end_time: int) -> Iterator[Dict[str, Any]]:
        if not self.session:
            raise SQLClientError("Failed to create database session")

        try:
            query = text(self.integration_query)  # type: ignore
            result = self.session.execute(
                query, {"start_time": start_time, "end_time": end_time}
            )

            for row in result:
                timestamp = self._format_timestamp(row._mapping["timestamp"])
                values = {
                    k: v
                    for k, v in row._mapping.items()
                    if k not in ["timestamp", "customer_id"]
                }

                yield {
                    "customer_id": str(row._mapping["customer_id"]),
                    "timestamp": timestamp,
                    "values": values,
                }

            logger.info(
                "Fetched metrics from SQL Client",
                start_time=start_time,
                end_time=end_time,
                start_time_str=datetime.fromtimestamp(
                    start_time, timezone.utc
                ).strftime("%Y-%m-%d %H:%M:%S"),
                end_time_str=datetime.fromtimestamp(end_time, timezone.utc).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
            )

        except Exception as e:
            logger.error(f"Failed to query SQL metrics: {str(e)}", exc_info=True)
            raise SQLClientError(f"Failed to query metrics: {str(e)}") from e
        finally:
            self.close()

    def query_logs(
        self, start_time: PositiveInt, end_time: PositiveInt
    ) -> Iterator[Dict[str, Any]]:
        raise NotImplementedError(
            "SQL logs are not supported. Use `metrics` flag instead."
        )
