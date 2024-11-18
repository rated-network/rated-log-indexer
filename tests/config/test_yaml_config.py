from pathlib import Path

import pytest
import os
from unittest.mock import mock_open, patch

import yaml
from pydantic import ValidationError
from rated_exporter_sdk.providers.prometheus.types import Step, TimeUnit  # type: ignore

from src.config.manager import RatedIndexerYamlConfig, FileConfigurationManager
from src.config.models.offset import OffsetYamlConfig
from src.config.models.inputs.input import InputYamlConfig
from src.config.models.output import OutputYamlConfig
from src.config.models.secrets import SecretsYamlConfig


def test_load_config_valid(valid_config_yaml, postgres_container, tmp_path):
    with tmp_path.joinpath("config.yml").open("w") as config_file:
        config_file.write(valid_config_yaml)

    config = FileConfigurationManager(Path(config_file.name)).load_config()

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
    assert config.inputs[0].cloudwatch.logs_config.log_group_name == "my-log-group"

    assert config.output.type == "rated"
    assert config.output.rated.ingestion_id == "6fa9df30-3746-4f73-b730-5f717ea0d56f"
    assert config.output.rated.ingestion_key == "ingestKEYOXVnw6deLQ5AQ"

    assert config.inputs[0].offset.type == "postgres"
    assert config.inputs[0].offset.start_from == 123456789
    assert config.inputs[0].offset.start_from_type == "bigint"
    assert config.inputs[0].offset.postgres.table_name == "offset_tracking"
    assert config.inputs[0].offset.postgres.host == "localhost"
    assert config.inputs[0].offset.postgres.port == int(
        postgres_container.get_exposed_port(5432)
    )
    assert config.inputs[0].offset.postgres.database == "test"
    assert config.inputs[0].offset.postgres.user == "test"
    assert config.inputs[0].offset.postgres.password == "test"

    assert config.secrets.use_secrets_manager is False


def test_load_config_file_not_found(tmp_path):
    not_existing_file = tmp_path.joinpath("not_existing_file.yml")
    with pytest.raises(FileNotFoundError):
        FileConfigurationManager(not_existing_file).load_config()


def test_get_config_invalid_input_type(valid_config_dict, tmp_path):
    invalid_config = valid_config_dict.copy()
    invalid_config["inputs"][0] = {
        "type": "invalid_source",
    }

    with pytest.raises(ValueError) as exc_info:
        RatedIndexerYamlConfig(**invalid_config)

    error_message = str(exc_info.value)
    assert "1 validation error for RatedIndexerYamlConfig" in error_message
    assert (
        'Invalid input source found "invalid_source": please use one of'
        in error_message
    )


def test_get_config_missing_input_config(valid_config_dict, tmp_path):
    invalid_config = valid_config_dict.copy()
    invalid_config["inputs"][0] = {
        "integration": "cloudwatch",
    }

    with pytest.raises(ValueError) as exc_info:
        RatedIndexerYamlConfig(**invalid_config)

    error_message = str(exc_info.value)
    assert (
        'Configuration for input source "cloudwatch" is not found. Please add input configuration for cloudwatch.'
        in error_message
    )


@pytest.mark.parametrize(
    "step_value, step_unit, should_pass",
    [
        (30, TimeUnit.SECONDS, True),
        (15, TimeUnit.SECONDS, True),
        (20, TimeUnit.SECONDS, True),
        (500, TimeUnit.MILLISECONDS, False),
        (45, TimeUnit.SECONDS, False),
        (90, TimeUnit.SECONDS, False),
        (1, TimeUnit.MINUTES, True),
        (750, TimeUnit.MILLISECONDS, False),
    ],
)
def test_prometheus_config_step_validation(
    valid_prometheus_config_dict, postgres_container, step_value, step_unit, should_pass
):
    # Modify the step in the config dictionary
    config_dict = valid_prometheus_config_dict.copy()
    config_dict["inputs"][0]["prometheus"]["queries"][0]["step"] = {
        "value": step_value,
        "unit": step_unit.value,
    }

    # Convert to YAML
    yaml_content = yaml.dump(config_dict)

    with patch("builtins.open", mock_open(read_data=yaml_content)):
        with patch.object(os.path, "exists", return_value=True):
            if should_pass:
                config = RatedIndexerYamlConfig(**config_dict)
                assert config.inputs[0].prometheus.queries[0].step == Step(
                    value=step_value, unit=step_unit
                )
            else:
                with pytest.raises(ValidationError):
                    RatedIndexerYamlConfig(**config_dict)
