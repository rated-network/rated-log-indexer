import pytest
from pydantic import ValidationError

from src.config.models.inputs.input import InputTypes
from src.config.manager import RatedIndexerYamlConfig


def test_different_integration_prefixes_no_error(valid_config_dict):
    valid_config_dict["inputs"].append(valid_config_dict["inputs"][0].copy())
    valid_config_dict["inputs"][0]["integration_prefix"] = "prefix1"
    valid_config_dict["inputs"][1]["integration_prefix"] = "prefix2"

    try:
        RatedIndexerYamlConfig(**valid_config_dict)
    except ValidationError:
        pytest.fail("Validation error raised unexpectedly")


def test_filters_required_for_logs(valid_config_dict):
    valid_config_dict["inputs"][0]["type"] = InputTypes.LOGS
    valid_config_dict["inputs"][0]["filters"] = None

    with pytest.raises(
        ValidationError, match="'filters' is mandatory when input type is LOGS"
    ):
        RatedIndexerYamlConfig(**valid_config_dict)


def test_filters_none_for_metrics_no_error(valid_config_dict):
    valid_config_dict["inputs"][0]["type"] = InputTypes.METRICS
    valid_config_dict["inputs"][0]["filters"] = None

    try:
        RatedIndexerYamlConfig(**valid_config_dict)
    except ValidationError:
        pytest.fail("Validation error raised unexpectedly")
