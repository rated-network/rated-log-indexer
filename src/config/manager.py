import sys
from typing import List, Optional
import structlog
import yaml
import os

from src.config.models.sentry import SentryYamlConfig
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
    sentry: Optional[SentryYamlConfig] = None

    @model_validator(mode="after")
    def check_integration_prefixes(cls, values):
        if not hasattr(values, "_duplicate_warning_logged"):
            values._duplicate_warning_logged = False

        integration_prefixes = [input.integration_prefix for input in values.inputs]

        duplicates = set(
            [x for x in integration_prefixes if integration_prefixes.count(x) > 1]
        )
        if duplicates and not values._duplicate_warning_logged:
            values._duplicate_warning_logged = True
            logger.warning(
                f"Duplicate integration_prefix values found: {', '.join(duplicates)}. Please make sure this is the intended behavior. This will send data from multiple integrations to the same datastream `key`. Sleeping 10s ..."
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

        try:
            with open(config_path, "r") as file:
                config_data = yaml.safe_load(file)

            config = RatedIndexerYamlConfig(**config_data)

            if config.secrets.use_secrets_manager:
                ConfigurationManager._resolve_secrets(config)

            return config

        except ValidationError as e:
            logger.error("Configuration validation failed:", exc_info=e)
            for error in e.errors():
                logger.error(f"Field: {error['loc']} - Error: {error['msg']}")
            sys.exit(1)
        except yaml.YAMLError as e:
            logger.error("Error parsing YAML configuration:", exc_info=e)
            sys.exit(1)
        except Exception as e:
            logger.error(
                "An unexpected error occurred while loading the configuration:",
                exc_info=e,
            )
            sys.exit(1)

    @staticmethod
    def _resolve_secrets(config: RatedIndexerYamlConfig):
        try:
            secret_manager_handler = SecretManagerFactory.create(config)
            secret_manager_handler.resolve_secrets(config)
        except KeyError as e:
            logger.error(f"Error resolving secret: {str(e)}")
            logger.info(
                "Please check your secret key and ensure it exists in the secret manager."
            )
            sys.exit(1)
        except ValueError as e:
            logger.error(f"Invalid secret format: {str(e)}")
            logger.info("Please check your secret format in the configuration file.")
            sys.exit(1)
        except Exception as e:
            logger.error(
                "An unexpected error occurred while resolving secrets:", exc_info=e
            )
            sys.exit(1)
