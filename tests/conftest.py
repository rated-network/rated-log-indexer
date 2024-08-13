from unittest.mock import patch

import pytest
import yaml

from src.config.manager import ConfigurationManager


@pytest.fixture
def valid_config_dict():
    return {
        "input": {
            "type": "cloudwatch",
            "cloudwatch": {
                "region": "us-east-1",
                "log_group_name": "my-log-group",
                "aws_access_key_id": "fake_access_key",
                "aws_secret_access_key": "fake_secret_key",
            },
        },
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
                    "key": "timestamp",
                    "value": "2023-08-07T10:15:30Z",
                    "field_type": "timestamp",
                    "format": "%Y-%m-%dT%H:%M:%SZ",
                    "path": "payload.timestamp",
                },
                {
                    "key": "level",
                    "value": "INFO",
                    "field_type": "string",
                    "path": "payload.level",
                },
                {
                    "key": "message",
                    "value": "User logged in",
                    "field_type": "string",
                    "path": "payload.message",
                },
            ],
        },
        "output": {
            "type": "rated",
            "rated": {
                "ingestion_id": "your_ingestion_id",
                "ingestion_key": "your_ingestion_key",
                "ingestion_url": "https://your_ingestion_url.com",
            },
        },
        "offset": {
            "type": "postgres",
            "start_from": 123456789,
            "start_from_type": "bigint",
            "postgres": {
                "table_name": "offset_tracking",
                "host": "db",
                "port": 5432,
                "database": "test_db",
                "user": "user",
                "password": "password",
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
