from unittest.mock import patch

import pytest
import yaml

from src.config.manager import ConfigurationManager


@pytest.fixture
def valid_config_dict():
    return {
        "inputs": [
            {
                "integration": "cloudwatch",
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
                        "host": "db",
                        "port": 5432,
                        "database": "test_db",
                        "user": "user",
                        "password": "password",
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
def valid_config_yaml(valid_config_dict):
    return yaml.dump(valid_config_dict)


@pytest.fixture
def mock_load_config():
    with patch.object(ConfigurationManager, "load_config") as mock_load_config:
        yield mock_load_config
