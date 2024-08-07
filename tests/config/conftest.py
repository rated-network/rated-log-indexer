import pytest
import yaml


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
        "output": {
            "type": "rated",
            "rated": {
                "slaos_api_key": "your_slaos_api_key",
                "ingestion_id": "your_ingestion_id",
                "ingestion_key": "your_ingestion_key",
            },
        },
        "offset": {
            "type": "postgres",
            "start_from": 123456789,
            "start_from_type": "bigint",
            "postgres": {
                "table_name": "offset_tracking",
                "host": "localhost",
                "port": 5432,
                "database": "postgres",
                "user": "postgres",
                "password": "postgres",
            },
        },
        "secrets": {"use_secrets_manager": False},
    }


@pytest.fixture
def valid_config_yaml(valid_config_dict):
    return yaml.dump(valid_config_dict)
