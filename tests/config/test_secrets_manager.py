import json
import pytest
from unittest.mock import patch

from src.config.models.inputs.input import IntegrationTypes
from src.config.manager import RatedIndexerYamlConfig
from src.config.models.secrets import SecretProvider
from src.config.secrets.aws_secrets_manager import AwsSecretManager


@pytest.fixture
def valid_config_with_secrets(valid_config_dict):
    config = valid_config_dict.copy()
    config["secrets"] = {
        "use_secrets_manager": True,
        "provider": "aws",
        "aws": {
            "region": "us-west-2",
            "aws_access_key_id": "fake_access_key",
            "aws_secret_access_key": "fake_secret_key",
        },
    }

    config["inputs"] = [
        {
            "integration": "datadog",
            "type": "logs",
            "slaos_key": "secret:slaos_key_secret",
            "datadog": {
                "api_key": "secret:datadog_api_key_in_secrets_manager",
                "app_key": "app_key_value_raw",
                "site": "datadog.eu",
                "logs_config": {
                    "query": "some_query",
                    "indexes": ["main"],
                },
            },
            "filters": {
                "version": 1,
                "log_format": "json_dict",
                "log_example": {},
                "fields": [
                    {
                        "key": "organization_id",
                        "field_type": "string",
                        "path": "path1",
                    }
                ],
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
    ]
    config["output"]["rated"][
        "ingestion_key"
    ] = "secret:ingestion_key_key_in_secrets_manager"
    return config


@pytest.fixture
def mock_aws_secrets_manager():
    with patch("src.config.secrets.aws_secrets_manager.boto3.client") as mock_client:
        mock_secrets = {
            "datadog_api_key_in_secrets_manager": "resolved_datadog_api_key",
            "ingestion_key_key_in_secrets_manager": "resolved_ingestion_key",
            "app_key": "resolved_datadog_app_key",
            "dict_secret": json.dumps({"key1": "value1", "key2": "value2"}),
            "string_secret": "just_a_string",
            "slaos_key_secret": "resolved_slaos_key",
        }
        mock_client.return_value.get_secret_value.side_effect = lambda SecretId: {
            "SecretString": mock_secrets[SecretId]
        }
        yield mock_client


def test_get_config_with_secrets(valid_config_with_secrets, mock_aws_secrets_manager):
    result_config = RatedIndexerYamlConfig(**valid_config_with_secrets)
    secret_manager = AwsSecretManager(result_config.secrets.aws)
    secret_manager.resolve_secrets(result_config)

    assert isinstance(result_config, RatedIndexerYamlConfig)
    assert result_config.secrets.use_secrets_manager is True
    assert result_config.secrets.provider == SecretProvider.AWS
    assert result_config.inputs[0].integration == IntegrationTypes.DATADOG
    assert result_config.inputs[0].datadog.app_key == "app_key_value_raw"
    assert result_config.inputs[0].datadog.api_key == "resolved_datadog_api_key"
    assert result_config.inputs[0].slaos_key == "resolved_slaos_key"
    assert result_config.output.rated.ingestion_key == "resolved_ingestion_key"

    assert not result_config.inputs[0].datadog.api_key.startswith("secret:")
    assert not result_config.output.rated.ingestion_key.startswith("secret:")


def test_secret_manager_resolve_secrets(
    valid_config_with_secrets, mock_aws_secrets_manager
):
    secret_name = "secret:app_key"
    valid_config_with_secrets["inputs"][0]["datadog"]["app_key"] = secret_name

    config = RatedIndexerYamlConfig(**valid_config_with_secrets)
    secret_manager = AwsSecretManager(config.secrets.aws)
    secret_manager.resolve_secrets(config)

    assert config.inputs[0].datadog.app_key == "resolved_datadog_app_key"
    assert config.inputs[0].datadog.api_key != secret_name


def test_secret_manager_resolve_dictionary_secret(
    valid_config_with_secrets, mock_aws_secrets_manager
):
    secret_name = "secret|key1:dict_secret"
    valid_config_with_secrets["inputs"][0]["datadog"]["app_key"] = secret_name

    config = RatedIndexerYamlConfig(**valid_config_with_secrets)
    secret_manager = AwsSecretManager(config.secrets.aws)
    secret_manager.resolve_secrets(config)

    assert config.inputs[0].datadog.api_key != secret_name
    assert config.inputs[0].datadog.app_key == "value1"


def test_secret_manager_resolve_string_as_dictionary_raises_error(
    valid_config_with_secrets, mock_aws_secrets_manager
):
    secret_name = "secret|key1:string_secret"
    valid_config_with_secrets["inputs"][0]["datadog"]["app_key"] = secret_name

    config = RatedIndexerYamlConfig(**valid_config_with_secrets)
    secret_manager = AwsSecretManager(config.secrets.aws)

    with pytest.raises(
        ValueError,
        match="Secret string_secret for app_key does not resolve to a dictionary",
    ):
        secret_manager.resolve_secrets(config)


def test_secret_manager_resolve_nonexistent_key_in_dictionary_raises_error(
    valid_config_with_secrets, mock_aws_secrets_manager
):
    secret_name = "secret|nonexistent_key:dict_secret"
    valid_config_with_secrets["inputs"][0]["datadog"]["app_key"] = secret_name

    config = RatedIndexerYamlConfig(**valid_config_with_secrets)
    secret_manager = AwsSecretManager(config.secrets.aws)

    with pytest.raises(
        KeyError,
        match="Key 'nonexistent_key' not found in secret dict_secret for app_key. Available keys: key1, key2",
    ):
        secret_manager.resolve_secrets(config)
