import base64
import copy
import json
import os

import pytest
import yaml

from src.config.manager import get_config_manager, RatedIndexerYamlConfig, get_config


def test_config_manager_can_load_base64_encoded_config(valid_config_dict):
    base64_encoded_config = base64.encodebytes(
        json.dumps(valid_config_dict).encode()
    ).decode()
    config = get_config_manager({"BASE64_CONFIG": base64_encoded_config}).load_config()

    assert isinstance(config, RatedIndexerYamlConfig)
    assert config.inputs[0].integration.value == "cloudwatch"


def test_load_from_new_location(test_config, config_paths):
    get_config.cache_clear()
    with config_paths["new_config_path"].open("w") as f:
        yaml.dump(test_config, f)

    os.chdir(config_paths["root_dir"])
    config = get_config()
    assert config is not None


def test_load_from_old_location(test_config, config_paths):
    get_config.cache_clear()
    with config_paths["old_config_path"].open("w") as f:
        yaml.dump(test_config, f)

    os.chdir(config_paths["root_dir"])
    config = get_config()
    assert config is not None
    # Could add assertion for warning log


def test_env_variable_override(test_config, config_paths):
    get_config.cache_clear()
    with config_paths["old_config_path"].open("w") as f:
        yaml.dump(test_config, f)

    os.environ["CONFIG_FILE"] = str(config_paths["old_config_path"])
    config = get_config()
    assert config is not None
    del os.environ["CONFIG_FILE"]


def test_prefer_new_location(test_config, config_paths):
    get_config.cache_clear()

    new_location_config = copy.deepcopy(test_config)
    new_location_config["inputs"][0]["slaos_key"] = "new_location_test"

    old_location_config = copy.deepcopy(test_config)
    old_location_config["inputs"][0]["slaos_key"] = "old_location_test"

    with config_paths["new_config_path"].open("w") as f:
        yaml.dump(new_location_config, f)

    with config_paths["old_config_path"].open("w") as f:
        yaml.dump(old_location_config, f)

    os.chdir(config_paths["root_dir"])
    config = get_config()
    assert config is not None
    assert (
        config.inputs[0].slaos_key == "new_location_test"
    ), "Should load from new location"


def test_no_config_found(config_paths):
    get_config.cache_clear()
    os.chdir(config_paths["root_dir"])
    with pytest.raises(FileNotFoundError):
        get_config()
