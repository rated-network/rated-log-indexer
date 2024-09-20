from abc import ABC, abstractmethod
from typing import Any, Union, Dict
import structlog
from pydantic import BaseModel

logger = structlog.get_logger(__name__)


class SecretManager(ABC):
    @abstractmethod
    def resolve_secret(self, secret_id: str) -> Union[str, Dict[str, Any]]:
        pass

    def resolve_secrets(self, config) -> None:
        logger.info("Starting secrets resolution process")
        self._resolve_secrets_in_object(config)
        logger.info("Completed secrets resolution process")

    def _resolve_secrets_in_object(self, obj):
        if isinstance(obj, BaseModel):
            for field in obj.__fields__:
                value = getattr(obj, field)
                resolved_value = self._resolve_value(value, field)
                setattr(obj, field, resolved_value)
        elif isinstance(obj, dict):
            for key, value in obj.items():
                obj[key] = self._resolve_value(value, key)
        elif isinstance(obj, list):
            for i, value in enumerate(obj):
                obj[i] = self._resolve_value(value, f"index {i}")

    def _resolve_value(self, value: Any, context: str) -> Any:
        if isinstance(value, (BaseModel, dict, list)):
            self._resolve_secrets_in_object(value)
            return value
        elif isinstance(value, str) and value.startswith("secret"):
            parts = value.split(":", 1)
            if len(parts) != 2:
                raise ValueError(f"Invalid secret format for {context}: {value}")

            secret_info, secret_id = parts
            try:
                resolved_value = self.resolve_secret(secret_id)
            except Exception as e:
                raise ValueError(f"Error resolving secret for {context}: {str(e)}")

            if "|" in secret_info:
                _, dict_key = secret_info.split("|")
                if not isinstance(resolved_value, dict):
                    raise ValueError(
                        f"Secret {secret_id} for {context} does not resolve to a dictionary"
                    )

                if dict_key not in resolved_value:
                    available_keys = ", ".join(resolved_value.keys())
                    raise KeyError(
                        f"Key '{dict_key}' not found in secret {secret_id} for {context}. Available keys: {available_keys}"
                    )

                logger.info(
                    f"Resolved secret '{secret_id}' using key '{dict_key}' for {context}"
                )
                return resolved_value[dict_key]
            else:
                if isinstance(resolved_value, dict):
                    raise ValueError(
                        f"Secret {secret_id} for {context} resolves to a dictionary, but no key was specified"
                    )

                logger.info(f"Resolved secret '{secret_id}' as a string for {context}")
                return resolved_value

        return value
