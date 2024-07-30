from typing import Union
from datetime import datetime
from sqlalchemy import Table, Column, MetaData, Integer, DateTime, BigInteger
from sqlalchemy.sql import select, update
from .base import OffsetTracker
from models.configs.postgres_config import PostgresConfig
from src.clients.postgres import PostgresClient


class PostgresOffsetTracker(OffsetTracker):
    def __init__(self):
        super().__init__()
        if self.config.type != "postgres":
            raise ValueError(
                "Offset tracker type is not set to 'postgres' in the configuration"
            )

        postgres_config = PostgresConfig(
            DB_DSN=f"postgresql://{self.config.postgres.user}:{self.config.postgres.password}@{self.config.postgres.host}:{self.config.postgres.port}/{self.config.postgres.database}"
        )
        self.client = PostgresClient(postgres_config)
        self.table_name = self.config.postgres.table_name
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        metadata = MetaData()

        if self.config.start_from_type == "bigint":
            offset_column_type = BigInteger
        elif self.config.start_from_type == "datetime":
            offset_column_type = DateTime
        else:
            raise ValueError(f"Invalid start_from_type: {self.config.start_from_type}")

        self.table = Table(
            self.table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("current_offset", offset_column_type),
        )
        metadata.create_all(self.client.engine)

        # Insert initial row if not exists
        with self.client.engine.connect() as connection:
            select_stmt = self.table.select()
            result = connection.execute(select_stmt)
            if result.fetchone() is None:
                insert_stmt = self.table.insert().values(
                    current_offset=self.config.start_from
                )
                connection.execute(insert_stmt)
                connection.commit()

    def get_current_offset(self) -> Union[int, datetime]:
        with self.client.engine.connect() as connection:
            select_stmt = select(self.table.c.current_offset)
            result = connection.execute(select_stmt)
            row = result.fetchone()
            if row and row[0] is not None:
                return row[0]
            return self.start_from

    def update_offset(self, offset: Union[int, datetime]) -> None:
        with self.client.engine.connect() as connection:
            update_stmt = update(self.table).values(current_offset=offset)
            connection.execute(update_stmt)
            connection.commit()
