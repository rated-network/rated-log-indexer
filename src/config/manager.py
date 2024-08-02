import structlog
import yaml
import os

from src.config.secrets.factory import SecretManagerFactory
from src.config.models.offset import OffsetYamlConfig
from src.config.models.input import InputYamlConfig
from src.config.models.output import OutputYamlConfig
from src.config.models.secrets import SecretsYamlConfig
from pydantic import BaseModel


logger = structlog.get_logger(__name__)


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
            config = RatedIndexerYamlConfig(**config_data)
            if config.secrets.use_secrets_manager:
                secret_manager_handler = SecretManagerFactory.create(config)
                secret_manager_handler.resolve_secrets(config)
            else:
                logger.warning(
                    "Secrets manager is disabled, its use is encouraged in production environments"
                )
            return config
        except ValueError as e:
            raise ValueError(f"Invalid configuration: {str(e)}")
