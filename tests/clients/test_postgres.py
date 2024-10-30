from datetime import datetime, timedelta, timezone, UTC

import pytest
from sqlalchemy import text, create_engine
from sqlalchemy.orm import Session
from src.clients.postgres import PostgresConfig, PostgresClient, SQLClientError


def valid_config():
    config = PostgresConfig(
        host="localhost",
        port=5432,
        user="user",
        database="test_db",
        password="password",
        dsn="",
    )
    return config


def invalid_config():
    return {
        "host": "localhost",
        "port": "invalid_port",
        "user": "user",
        "database": "test_db",
        "password": "password",
    }


def test_creates_valid_dsn():
    config = valid_config()
    assert config.dsn == "postgresql://user:password@localhost:5432/test_db"


def test_raises_error_on_invalid_config():
    with pytest.raises(TypeError):
        PostgresConfig(**invalid_config())


def test_creates_session_successfully():
    config = valid_config()
    client = PostgresClient(config)
    assert isinstance(client.session, Session)
    client.close()


def test_closes_session_successfully():
    config = valid_config()
    client = PostgresClient(config)
    client.close()
    assert client.session is None


@pytest.fixture(scope="function")
def postgres_client_with_table():
    config = valid_config()
    engine = create_engine(config.dsn)
    client = PostgresClient(config)

    # Create the test_metrics table
    with engine.connect() as connection:
        connection.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS test_metrics (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP,
                customer_id VARCHAR(50),
                value1 FLOAT,
                value2 INTEGER
            )
        """
            )
        )
        connection.commit()

    yield client

    # Drop the test_metrics table after the test
    with engine.connect() as connection:
        connection.execute(text("DROP TABLE IF EXISTS test_metrics"))
        connection.commit()

    client.close()


@pytest.fixture(scope="function")
def postgres_client_with_data(postgres_client_with_table):
    client = postgres_client_with_table
    try:
        with client.engine.connect() as conn:
            current_time = datetime.now(timezone.utc)
            for i in range(100):
                timestamp = current_time - timedelta(hours=i)
                conn.execute(
                    text(
                        """
                    INSERT INTO test_metrics (timestamp, customer_id, value1, value2)
                    VALUES (:timestamp, :customer_id, :value1, :value2)
                """
                    ),
                    {
                        "timestamp": timestamp,
                        "customer_id": f"customer_{i % 10}",
                        "value1": float(i),
                        "value2": i,
                    },
                )
            conn.commit()
        yield client
    finally:
        with client.engine.connect() as conn:
            conn.execute(text("DELETE FROM test_metrics"))
            conn.commit()


def test_validate_query_table_not_exists():
    config = valid_config()
    client = PostgresClient(config)
    try:
        client.integration_query = "SELECT * FROM non_existent_table"
        with pytest.raises(SQLClientError, match="relation .* does not exist"):
            client._validate_query()
    finally:
        client.close()


def test_validate_query_invalid_syntax(postgres_client_with_table):
    postgres_client_with_table.integration_query = "SELECT * FROM"
    with pytest.raises(SQLClientError, match="SQL syntax error:"):
        postgres_client_with_table._validate_query()


def test_validate_query_missing_required_columns(postgres_client_with_table):
    postgres_client_with_table.integration_query = (
        "SELECT id, value1, value2 FROM test_metrics"
    )
    with pytest.raises(
        SQLClientError,
        match="Query result must include 'timestamp' and 'customer_id' columns",
    ):
        postgres_client_with_table._validate_query()


def test_validate_query_relation_does_not_exist():
    config = valid_config()
    client = PostgresClient(config)
    try:
        client.integration_query = "SELECT * FROM non_existent_table"
        with pytest.raises(SQLClientError, match="relation .* does not exist"):
            client._validate_query()
    finally:
        client.close()


def test_validate_query_no_additional_columns(postgres_client_with_table):
    config = valid_config()
    client = PostgresClient(config)
    try:
        client.integration_query = "SELECT timestamp, customer_id FROM test_metrics"
        with pytest.raises(
            SQLClientError, match="Query should return additional columns for values"
        ):
            client._validate_query()
    finally:
        client.close()


# def test_validate_query_invalid_timestamp_type(postgres_client_with_table):
#     config = valid_config()
#     client = PostgresClient(config)
#     try:
#         client.integration_query = "SELECT timestamp::VARCHAR as timestamp, customer_id, value1 FROM test_metrics"
#         with pytest.raises(SQLClientError, match="Timestamp must be a DateTime, Integer, or Float type"):
#             client._validate_query()
#     finally:
#         client.close()


def test_query_metrics_successful(postgres_client_with_data):
    config = valid_config()
    client = PostgresClient(config)
    try:
        client.integration_query = """
        SELECT timestamp, customer_id, value1, value2
        FROM test_metrics
        WHERE timestamp BETWEEN
            to_timestamp(:start_time)::timestamp
            AND to_timestamp(:end_time)::timestamp
        """

        start_time = int((datetime.now(timezone.utc) - timedelta(days=1)).timestamp())
        end_time = int(datetime.now(timezone.utc).timestamp())

        results = list(client.query_metrics(start_time, end_time))

        assert len(results) > 0
        for result in results:
            assert set(result.keys()) == {"customer_id", "timestamp", "values"}
            assert isinstance(result["customer_id"], str)
            assert isinstance(result["timestamp"], str)
            assert isinstance(result["values"]["value1"], float)
            assert isinstance(result["values"]["value2"], int)
    finally:
        client.close()


def test_query_metrics_no_results(postgres_client_with_table):
    config = valid_config()
    client = PostgresClient(config)
    try:
        client.integration_query = """
        SELECT timestamp, customer_id, value1, value2
        FROM test_metrics
        WHERE timestamp BETWEEN
            to_timestamp(:start_time)::timestamp
            AND to_timestamp(:end_time)::timestamp
        """

        future_time = int(datetime.now(timezone.utc).timestamp()) + 24 * 60 * 60
        far_future_time = future_time + 24 * 60 * 60

        results = list(client.query_metrics(future_time, far_future_time))
        assert len(results) == 0
    finally:
        client.close()


def test_query_metrics_invalid_query():
    config = valid_config()
    client = PostgresClient(config)
    try:
        client.integration_query = "SELECT * FROM non_existent_table"

        start_time = int((datetime.now(UTC) - timedelta(days=1)).timestamp())
        end_time = int(datetime.now(UTC).timestamp())

        with pytest.raises(SQLClientError, match="Failed to query metrics"):
            list(client.query_metrics(start_time, end_time))
    finally:
        client.close()
