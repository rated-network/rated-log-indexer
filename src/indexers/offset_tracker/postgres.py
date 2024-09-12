from typing import cast, Type

from pydantic import StrictStr
from sqlalchemy import Table, Column, MetaData, Integer, BigInteger, String
from sqlalchemy.sql import select, update
from sqlalchemy.sql.type_api import TypeEngine

from src.config.models.offset import OffsetYamlConfig
from .base import OffsetTracker
from src.clients.postgres import PostgresClient, PostgresConfig


class PostgresOffsetTracker(OffsetTracker):
    def __init__(self, config: OffsetYamlConfig, integration_prefix: StrictStr):
        super().__init__(config=config, integration_prefix=integration_prefix)
        self.config = config
        self.integration_prefix = integration_prefix

        if self.config.type != "postgres":
            raise ValueError(
                "Offset tracker type is not set to 'postgres' in the configuration"
            )

        assert self.config.postgres is not None

        postgres_config = PostgresConfig(
            host=self.config.postgres.host,
            port=self.config.postgres.port,
            user=self.config.postgres.user,
            database=self.config.postgres.database,
            password=self.config.postgres.password,
        )
        self.client = PostgresClient(postgres_config)
        self.table_name = cast(str, self.config.postgres.table_name)

        self._ensure_table_exists()
        self._override_applied = False

    def _ensure_table_exists(self):
        metadata = MetaData()
        offset_column_type: Type[TypeEngine]

        if self.config.start_from_type == "bigint":
            offset_column_type = BigInteger
        else:
            raise ValueError(f"Invalid start_from_type: {self.config.start_from_type}")

        self.table = Table(
            self.table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("integration_prefix", String, unique=True),
            Column("current_offset", offset_column_type),
        )
        metadata.create_all(self.client.engine)

        # Insert initial row if it doesn't exist
        with self.client.engine.connect() as connection:
            select_stmt = select(self.table).where(
                self.table.c.integration_prefix == self.integration_prefix
            )
            result = connection.execute(select_stmt)
            if result.fetchone() is None:
                insert_stmt = self.table.insert().values(
                    integration_prefix=self.integration_prefix,
                    current_offset=self.config.start_from,
                )
                connection.execute(insert_stmt)
                connection.commit()

    def get_current_offset(self) -> int:
        if self.config.override_start_from and not self._override_applied:
            self._override_applied = True
            self.update_offset(self.config.start_from)
            return self.config.start_from

        # Retrieve current offset from the database
        with self.client.engine.connect() as connection:
            select_stmt = select(self.table.c.current_offset).where(
                self.table.c.integration_prefix == self.integration_prefix
            )
            result = connection.execute(select_stmt)
            row = result.fetchone()
            if row and row[0] is not None:
                return row[0]
        return self.config.start_from

    def update_offset(self, offset: int) -> None:
        with self.client.engine.connect() as connection:
            # Check if the row exists
            select_stmt = select(self.table).where(
                self.table.c.integration_prefix == self.integration_prefix
            )
            result = connection.execute(select_stmt)
            existing_row = result.fetchone()

            if existing_row:
                update_stmt = (
                    update(self.table)
                    .where(self.table.c.integration_prefix == self.integration_prefix)
                    .values(current_offset=offset)
                )
                connection.execute(update_stmt)
            else:
                insert_stmt = self.table.insert().values(
                    integration_prefix=self.integration_prefix, current_offset=offset
                )
                connection.execute(insert_stmt)

            connection.commit()
