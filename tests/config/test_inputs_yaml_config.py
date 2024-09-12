import pytest
from pydantic import ValidationError

from src.config.manager import RatedIndexerYamlConfig


def test_duplicate_integration_prefix_raises_error(valid_config_dict):
    valid_config_dict["inputs"].append(valid_config_dict["inputs"][0].copy())
    valid_config_dict["inputs"][0]["integration_prefix"] = "prefix1"
    valid_config_dict["inputs"][1]["integration_prefix"] = "prefix1"

    with pytest.raises(
        ValueError, match="Duplicate integration_prefix values found: prefix1"
    ):
        RatedIndexerYamlConfig(**valid_config_dict)


def test_different_integration_prefixes_no_error(valid_config_dict):
    valid_config_dict["inputs"].append(valid_config_dict["inputs"][0].copy())
    valid_config_dict["inputs"][0]["integration_prefix"] = "prefix1"
    valid_config_dict["inputs"][1]["integration_prefix"] = "prefix2"

    try:
        RatedIndexerYamlConfig(**valid_config_dict)
    except ValidationError:
        pytest.fail("Validation error raised unexpectedly")
