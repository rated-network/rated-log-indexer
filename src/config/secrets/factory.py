from src.config.models.secrets import SecretProvider
from src.config.secrets.manager import SecretManager


class SecretManagerFactory:
    @staticmethod
    def create(config) -> SecretManager:
        if config.secrets.provider == SecretProvider.AWS:
            from src.config.secrets.aws_secrets_manager import AwsSecretManager

            return AwsSecretManager(config.secrets.aws)
        else:
            raise ValueError(f"Unsupported secrets provider: {config.secrets.provider}")
