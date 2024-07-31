import yaml
import os

from src.config.models.offset import OffsetYamlConfig
from src.config.models.input import InputYamlConfig
from src.config.models.output import OutputYamlConfig
from src.config.models.secrets import SecretsYamlConfig
from pydantic import BaseModel


class RatedIndexerYamlConfig(BaseModel):
    input: InputYamlConfig
    output: OutputYamlConfig
    offset: OffsetYamlConfig
    secrets: SecretsYamlConfig


class ConfigurationManager:
    @staticmethod
    def load_config() -> RatedIndexerYamlConfig:
        config_path = os.path.join(os.getcwd(), "rated-config.yaml")
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_path, "r") as file:
            config_data = yaml.safe_load(file)

        try:
            return RatedIndexerYamlConfig(**config_data)
        except ValueError as e:
            raise ValueError(f"Invalid configuration: {str(e)}")

    @staticmethod
    def get_config() -> RatedIndexerYamlConfig:
        config = ConfigurationManager.load_config()
        return config
