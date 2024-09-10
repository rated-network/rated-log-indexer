import sys
from typing import List

import structlog
import yaml
import os

from src.config.secrets.factory import SecretManagerFactory
from src.config.models.offset import OffsetYamlConfig
from src.config.models.inputs.input import InputYamlConfig
from src.config.models.output import OutputYamlConfig
from src.config.models.secrets import SecretsYamlConfig
from pydantic import BaseModel, ValidationError

logger = structlog.get_logger(__name__)


class RatedIndexerYamlConfig(BaseModel):
    inputs: List[InputYamlConfig]
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

            return config

        except ValidationError as e:
            logger.error("Configuration validation failed with the following errors:")
            for error in e.errors():
                logger.error(f"Field: {error['loc']} - Error: {error['msg']}")
            sys.exit(1)
        except Exception as e:
            logger.exception(
                "An unexpected error occurred while loading the configuration.",
                exc_info=e,
            )
            raise
