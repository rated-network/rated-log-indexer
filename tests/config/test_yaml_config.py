import pytest
import yaml
import os
from unittest.mock import mock_open, patch

from src.config.manager import ConfigurationManager, RatedIndexerYamlConfig
from src.config.models.offset import OffsetYamlConfig
from src.config.models.inputs.input import InputYamlConfig
from src.config.models.output import OutputYamlConfig
from src.config.models.secrets import SecretsYamlConfig


def test_load_config_valid(valid_config_yaml):
    with patch("builtins.open", mock_open(read_data=valid_config_yaml)):
        with patch.object(os.path, "exists", return_value=True):
            config = ConfigurationManager.load_config()

            assert config.inputs[0].cloudwatch is not None
            assert config.output.rated is not None
            assert config.inputs[0].offset.postgres is not None

            assert isinstance(config, RatedIndexerYamlConfig)
            assert isinstance(config.inputs[0], InputYamlConfig)
            assert isinstance(config.output, OutputYamlConfig)
            assert isinstance(config.inputs[0].offset, OffsetYamlConfig)
            assert isinstance(config.secrets, SecretsYamlConfig)

            assert config.inputs[0].integration == "cloudwatch"
            assert config.inputs[0].type == "logs"
            assert config.inputs[0].cloudwatch.region == "us-east-1"
            assert (
                config.inputs[0].cloudwatch.logs_config.log_group_name == "my-log-group"
            )

            assert config.output.type == "rated"
            assert (
                config.output.rated.ingestion_id
                == "6fa9df30-3746-4f73-b730-5f717ea0d56f"
            )
            assert config.output.rated.ingestion_key == "ingestKEYOXVnw6deLQ5AQ"

            assert config.inputs[0].offset.type == "postgres"
            assert config.inputs[0].offset.start_from == 123456789
            assert config.inputs[0].offset.start_from_type == "bigint"
            assert config.inputs[0].offset.postgres.table_name == "offset_tracking"
            assert config.inputs[0].offset.postgres.host == "db"
            assert config.inputs[0].offset.postgres.port == 5432
            assert config.inputs[0].offset.postgres.database == "test_db"
            assert config.inputs[0].offset.postgres.user == "user"
            assert config.inputs[0].offset.postgres.password == "password"

            assert config.secrets.use_secrets_manager is False


def test_load_config_file_not_found():
    with patch.object(os.path, "exists", return_value=False):
        with pytest.raises(FileNotFoundError):
            ConfigurationManager.load_config()


def test_load_config_invalid_yaml():
    invalid_yaml = "input:\n  type: cloudwatch\n  cloudwatch:\n    region: us-east-1\n    : invalid"
    with patch("builtins.open", mock_open(read_data=invalid_yaml)):
        with patch.object(os.path, "exists", return_value=True):
            with pytest.raises(yaml.YAMLError):
                ConfigurationManager.load_config()


def test_get_config_invalid_input_type(valid_config_dict):
    invalid_config = valid_config_dict.copy()
    invalid_config["inputs"][0] = {
        "type": "invalid_source",
    }

    with patch.object(ConfigurationManager, "load_config") as mock_load_config:
        with pytest.raises(ValueError) as exc_info:
            mock_load_config.return_value = RatedIndexerYamlConfig(**invalid_config)
            ConfigurationManager.load_config()

        error_message = str(exc_info.value)
        assert "1 validation error for RatedIndexerYamlConfig" in error_message
        assert (
            'Invalid input source found "invalid_source": please use one of'
            in error_message
        )


def test_get_config_missing_input_config(valid_config_dict):
    invalid_config = valid_config_dict.copy()
    invalid_config["inputs"][0] = {
        "integration": "cloudwatch",
    }

    with patch.object(ConfigurationManager, "load_config") as mock_load_config:
        with pytest.raises(ValueError) as exc_info:
            mock_load_config.return_value = RatedIndexerYamlConfig(**invalid_config)
            ConfigurationManager.load_config()

        error_message = str(exc_info.value)
        assert (
            'Configuration for input source "cloudwatch" is not found. Please add input configuration for cloudwatch.'
            in error_message
        )
