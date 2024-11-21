import base64
import sys
from functools import lru_cache
from pathlib import Path
from typing import List, Optional, cast
import structlog
import yaml
import os

from .models.sentry import SentryYamlConfig
from .secrets.factory import SecretManagerFactory
from .models.inputs.input import InputYamlConfig
from .models.output import OutputYamlConfig
from .models.secrets import SecretsYamlConfig
from pydantic import BaseModel, ValidationError, model_validator
import abc

logger = structlog.get_logger(__name__)


class RatedIndexerYamlConfig(BaseModel):

    inputs: List[InputYamlConfig]
    output: OutputYamlConfig
    secrets: SecretsYamlConfig
    sentry: Optional[SentryYamlConfig] = None

    @model_validator(mode="after")
    def check_slaos_keyes(cls, values):
        if not hasattr(values, "_duplicate_warning_logged"):
            values._duplicate_warning_logged = False

        slaos_keyes = [input.slaos_key for input in values.inputs]

        duplicates = set([x for x in slaos_keyes if slaos_keyes.count(x) > 1])
        if duplicates and not values._duplicate_warning_logged:
            values._duplicate_warning_logged = True
            logger.warning(
                f"Duplicate slaos_key values found: {', '.join(duplicates)}. Please make sure this is the intended behavior. This will send data from multiple integrations to the same datastream `key`."
            )

        for _input in values.inputs:
            if _input.slaos_key is None or _input.slaos_key.strip() == "":
                logger.warning(
                    "Empty slaos_key found in input configuration. Consider providing a value."
                )

        return values


class ConfigurationManager(abc.ABC):
    @abc.abstractmethod
    def _do_load_raw_config(self) -> dict: ...

    def load_config(self) -> RatedIndexerYamlConfig:
        config_data = self._do_load_raw_config()
        try:
            config = RatedIndexerYamlConfig(**config_data)

            if config.secrets.use_secrets_manager:
                self._resolve_secrets(config)

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


class Base64EncodedConfig(ConfigurationManager):
    def __init__(self, config_value: str):
        self._config_value = config_value

    def _do_load_raw_config(self) -> dict:
        return yaml.safe_load(base64.b64decode(self._config_value))


class FileConfigurationManager(ConfigurationManager):
    def __init__(self, config_path: Path):
        self._config_path = config_path

    def _do_load_raw_config(self) -> dict:
        if not self._config_path.exists():
            raise FileNotFoundError(
                f"Configuration file not found: {self._config_path}"
            )
        try:
            with self._config_path.open("r") as file:
                return yaml.safe_load(file)

        except Exception as e:
            logger.error(
                "An unexpected error occurred while loading the configuration:",
                exc_info=e,
            )
            sys.exit(1)


def get_config_manager(
    env: dict[str, str] = cast(dict[str, str], os.environ)
) -> ConfigurationManager:
    if base64_config := env.get("BASE64_CONFIG"):
        return Base64EncodedConfig(base64_config)

    config_file = env.get("CONFIG_FILE")
    if config_file:
        return FileConfigurationManager(Path(config_file))

    config_path = Path("config/rated-config.yaml")
    if config_path.exists():
        return FileConfigurationManager(config_path)

    legacy_path = Path("rated-config.yaml")
    if legacy_path.exists():
        logger.warning(
            "Using config from root directory. This is deprecated - please move config to /indexer/config/ directory"
        )
        return FileConfigurationManager(legacy_path)

    return FileConfigurationManager(config_path)


@lru_cache
def get_config() -> RatedIndexerYamlConfig:
    return get_config_manager().load_config()
