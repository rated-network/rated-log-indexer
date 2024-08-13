import pytest
import yaml
import os
from unittest.mock import mock_open, patch

from src.config.manager import ConfigurationManager, RatedIndexerYamlConfig
from src.config.models.offset import OffsetYamlConfig
from src.config.models.input import InputYamlConfig
from src.config.models.output import OutputYamlConfig
from src.config.models.secrets import SecretsYamlConfig


def test_load_config_valid(valid_config_yaml):
    with patch("builtins.open", mock_open(read_data=valid_config_yaml)):
        with patch.object(os.path, "exists", return_value=True):
            config = ConfigurationManager.load_config()

            assert config.input.cloudwatch is not None
            assert config.output.rated is not None
            assert config.offset.postgres is not None

            assert isinstance(config, RatedIndexerYamlConfig)
            assert isinstance(config.input, InputYamlConfig)
            assert isinstance(config.output, OutputYamlConfig)
            assert isinstance(config.offset, OffsetYamlConfig)
            assert isinstance(config.secrets, SecretsYamlConfig)

            assert config.input.type == "cloudwatch"
            assert config.input.cloudwatch.region == "us-east-1"
            assert config.input.cloudwatch.log_group_name == "my-log-group"

            assert config.output.type == "rated"
            assert config.output.rated.ingestion_id == "your_ingestion_id"
            assert config.output.rated.ingestion_key == "your_ingestion_key"

            assert config.offset.type == "postgres"
            assert config.offset.start_from == 123456789
            assert config.offset.start_from_type == "bigint"
            assert config.offset.postgres.table_name == "offset_tracking"
            assert config.offset.postgres.host == "db"
            assert config.offset.postgres.port == 5432
            assert config.offset.postgres.database == "test_db"
            assert config.offset.postgres.user == "user"
            assert config.offset.postgres.password == "password"

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
    invalid_config["input"] = {
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
    invalid_config["input"] = {
        "type": "cloudwatch",
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
