import pytest
from unittest.mock import patch

from src.config.models.inputs.input import IntegrationTypes
from src.config.manager import ConfigurationManager, RatedIndexerYamlConfig
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
                        "key": "customer_id",
                        "field_type": "string",
                        "path": "path1",
                    }
                ],
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
        }
        mock_client.return_value.get_secret_value.side_effect = lambda SecretId: {
            "SecretString": mock_secrets[SecretId]
        }
        yield mock_client


def test_get_config_with_secrets(valid_config_with_secrets, mock_aws_secrets_manager):
    with patch.object(ConfigurationManager, "load_config") as mock_load_config:
        config = RatedIndexerYamlConfig(**valid_config_with_secrets)
        secret_manager = AwsSecretManager(config.secrets.aws)
        secret_manager.resolve_secrets(config)
        mock_load_config.return_value = config

        result_config = ConfigurationManager.load_config()

        assert isinstance(result_config, RatedIndexerYamlConfig)
        assert result_config.secrets.use_secrets_manager is True
        assert result_config.secrets.provider == SecretProvider.AWS
        assert result_config.inputs[0]["integration"] == IntegrationTypes.DATADOG
        assert result_config.inputs[0]["datadog"]["app_key"] == "app_key_value_raw"
        assert (
            result_config.inputs[0]["datadog"]["api_key"] == "resolved_datadog_api_key"
        )
        assert result_config.output.rated.ingestion_key == "resolved_ingestion_key"

        assert not result_config.inputs[0]["datadog"]["api_key"].startswith("secret:")
        assert not result_config.output.rated.ingestion_key.startswith("secret:")


def test_secret_manager_resolve_secrets(
    valid_config_with_secrets, mock_aws_secrets_manager
):
    secret_name = "secret:app_key"
    valid_config_with_secrets["inputs"][0]["datadog"]["app_key"] = secret_name

    config = RatedIndexerYamlConfig(**valid_config_with_secrets)
    secret_manager = AwsSecretManager(config.secrets.aws)
    secret_manager.resolve_secrets(config)

    assert config.inputs[0]["datadog"]["app_key"] == "resolved_datadog_app_key"
    assert config.inputs[0]["datadog"]["api_key"] != secret_name
