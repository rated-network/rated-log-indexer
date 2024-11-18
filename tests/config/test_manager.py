import base64
import json
from src.config.manager import get_config_manager, RatedIndexerYamlConfig


def test_config_manager_can_load_base64_encoded_config(valid_config_dict):
    base64_encoded_config = base64.encodebytes(
        json.dumps(valid_config_dict).encode()
    ).decode()
    config = get_config_manager({"BASE64_CONFIG": base64_encoded_config}).load_config()

    assert isinstance(config, RatedIndexerYamlConfig)
    assert config.inputs[0].integration.value == "cloudwatch"
