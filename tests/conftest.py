from unittest.mock import patch

import pytest
import yaml
from testcontainers.postgres import PostgresContainer  # type: ignore
from testcontainers.redis import RedisContainer  # type: ignore

from src.config.manager import ConfigurationManager


@pytest.fixture(scope="module")
def postgres_container():
    with PostgresContainer() as postgres:
        yield postgres


@pytest.fixture(scope="module")
def redis_container():
    with RedisContainer() as redis:
        yield redis


@pytest.fixture
def valid_config_dict(postgres_container):
    return {
        "inputs": [
            {
                "integration": "cloudwatch",
                "slaos_key": "my-slaos-key",
                "type": "logs",
                "filters": {
                    "version": 1,
                    "log_format": "json_dict",
                    "log_example": {
                        "timestamp": "2023-08-07T10:15:30Z",
                        "level": "INFO",
                        "message": "User logged in",
                    },
                    "fields": [
                        {
                            "key": "example_key",
                            "value": "example_value",
                            "field_type": "string",
                            "path": "payload.example_key",
                        },
                        {
                            "key": "organization_id",
                            "value": "organization_id_value",
                            "field_type": "string",
                            "path": "payload.organization_id",
                        },
                    ],
                },
                "cloudwatch": {
                    "region": "us-east-1",
                    "aws_access_key_id": "fake_access_key",
                    "aws_secret_access_key": "fake_secret_key",
                    "logs_config": {
                        "log_group_name": "my-log-group",
                        "filter_pattern": "*",
                    },
                },
                "offset": {
                    "type": "postgres",
                    "start_from": 123456789,
                    "start_from_type": "bigint",
                    "postgres": {
                        "table_name": "offset_tracking",
                        "host": "localhost",
                        "port": int(postgres_container.get_exposed_port(5432)),
                        "database": "test",
                        "user": "test",
                        "password": "test",
                    },
                },
            }
        ],
        "output": {
            "type": "rated",
            "rated": {
                "ingestion_id": "6fa9df30-3746-4f73-b730-5f717ea0d56f",
                "ingestion_key": "ingestKEYOXVnw6deLQ5AQ",
                "ingestion_url": "https://your_ingestion_url.com/v1/ingest",
            },
        },
        "secrets": {"use_secrets_manager": False},
    }


@pytest.fixture
def valid_prometheus_config_dict():
    return {
        "inputs": [
            {
                "integration": "prometheus",
                "slaos_key": "prometheus_metrics",
                "type": "metrics",
                "prometheus": {
                    "base_url": "http://www.base-url.com:9090",
                    "queries": [
                        {
                            "query": "rate(test_requests_total[5m]) by (user_id)",
                            "step": {"value": 30, "unit": "s"},
                            "slaos_metric_name": "test_requests_rate",
                            "organization_identifier": "user_id",
                        }
                    ],
                    "timeout": 15.0,
                    "max_retries": 3,
                },
                "filters": {
                    "version": 1,
                    "log_format": "json_dict",
                    "log_example": {},
                    "fields": [
                        {
                            "key": "organization_id",
                            "value": "test_org",
                            "field_type": "string",
                            "path": "user_id",
                        },
                        {
                            "key": "instance",
                            "value": "0",
                            "field_type": "string",
                            "path": "instance",
                        },
                        {
                            "key": "region",
                            "value": "0",
                            "field_type": "string",
                            "path": "region",
                            "hash": True,
                        },
                    ],
                },
                "offset": {
                    "type": "redis",
                    "override_start_from": True,
                    "start_from": 1730729435241,
                    "start_from_type": "bigint",
                    "redis": {"host": "redis", "port": 6379, "db": 0},
                },
            },
        ],
        "output": {
            "type": "rated",
            "rated": {
                "ingestion_id": "ingestion_id",
                "ingestion_key": "ingestion_key",
                "ingestion_url": "https://your_ingestion_url.com/v1/ingest",
            },
        },
        "secrets": {"use_secrets_manager": False},
    }


@pytest.fixture
def valid_config_yaml(valid_config_dict):
    return yaml.dump(valid_config_dict)


@pytest.fixture
def mock_load_config():
    with patch.object(ConfigurationManager, "load_config") as mock_load_config:
        yield mock_load_config
