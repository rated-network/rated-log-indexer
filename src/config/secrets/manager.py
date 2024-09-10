from abc import ABC, abstractmethod
from typing import Dict, Any
import structlog
from pydantic import BaseModel

logger = structlog.get_logger(__name__)


class SecretManager(ABC):
    @abstractmethod
    def resolve_secret(self, secret_id: str) -> str:
        pass

    def resolve_secrets(self, config) -> None:
        logger.info("Starting secrets resolution process")
        config_dict = config.model_dump()
        updated_config = self._resolve_secrets_in_dict(config_dict)

        for key, value in updated_config.items():
            if key != "secrets":  # Skip the secrets configuration itself
                if isinstance(getattr(config, key), BaseModel):
                    self._update_nested_model(getattr(config, key), value)
                else:
                    setattr(config, key, value)

        logger.info("Completed secrets resolution process")

    def _update_nested_model(
        self, model: BaseModel, updated_data: Dict[str, Any]
    ) -> None:
        for key, value in updated_data.items():
            if isinstance(value, dict) and isinstance(
                getattr(model, key, None), BaseModel
            ):
                self._update_nested_model(getattr(model, key), value)
            else:
                setattr(model, key, value)

    def _resolve_secrets_in_dict(self, config_dict: Dict[str, Any]) -> Dict:
        def resolve_value(value):
            if isinstance(value, dict):
                return {k: resolve_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [resolve_value(item) for item in value]
            elif isinstance(value, str) and value.startswith("secret:"):
                secret_id = value.split(":", 1)[1]
                resolved_value = self.resolve_secret(secret_id)
                logger.info(
                    f"Resolved secret '{secret_id}' using provider '{self.__class__.__name__}'"
                )
                return resolved_value
            return value

        resolved_dict = {
            k: resolve_value(v) for k, v in config_dict.items() if k != "secrets"
        }

        return resolved_dict
