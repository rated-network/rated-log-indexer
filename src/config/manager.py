import sys
from typing import List

import structlog
import yaml
import os

from src.config.secrets.factory import SecretManagerFactory
from src.config.models.inputs.input import InputYamlConfig
from src.config.models.output import OutputYamlConfig
from src.config.models.secrets import SecretsYamlConfig
from pydantic import BaseModel, ValidationError, model_validator

logger = structlog.get_logger(__name__)


class RatedIndexerYamlConfig(BaseModel):
    inputs: List[InputYamlConfig]
    output: OutputYamlConfig
    secrets: SecretsYamlConfig

    @model_validator(mode="after")
    def check_integration_prefixes(cls, values):
        integration_prefixes = [input.integration_prefix for input in values.inputs]

        duplicates = set(
            [x for x in integration_prefixes if integration_prefixes.count(x) > 1]
        )
        if duplicates:
            raise ValueError(
                f"Duplicate integration_prefix values found: {', '.join(duplicates)}"
            )

        for _input in values.inputs:
            if (
                _input.integration_prefix is None
                or _input.integration_prefix.strip() == ""
            ):
                logger.warning(
                    "Empty integration_prefix found in input configuration. Consider providing a value."
                )

        return values


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
