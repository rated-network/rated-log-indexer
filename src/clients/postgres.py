from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from src.models.configs.postgres_config import PostgresConfig


class PostgresClient:
    """
    Reads and writes to postgres database
    """

    def __init__(self, config: PostgresConfig):
        self.engine = create_engine(config.DB_DSN)
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
