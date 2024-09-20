import boto3  # type: ignore
from botocore.exceptions import ClientError  # type: ignore
from pydantic import BaseModel, StrictStr
import structlog
import json
from typing import Union, Dict, Any

from src.config.secrets.manager import SecretManager

logger = structlog.get_logger(__name__)


class AwsSecretsManagerConfig(BaseModel):
    region: StrictStr
    aws_access_key_id: StrictStr
    aws_secret_access_key: StrictStr


class AwsSecretManager(SecretManager):
    def __init__(self, config: AwsSecretsManagerConfig):
        self.secrets_client = boto3.client(
            "secretsmanager",
            region_name=config.region,
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
        )

    def resolve_secret(self, secret_id: StrictStr) -> Union[str, Dict[str, Any]]:
        try:
            get_secret_value_response = self.secrets_client.get_secret_value(
                SecretId=secret_id
            )
        except ClientError as e:
            raise e

        if "SecretString" in get_secret_value_response:
            secret_string = get_secret_value_response["SecretString"]
            try:
                secret_value = json.loads(secret_string)
                return secret_value
            except json.JSONDecodeError:
                return secret_string
        else:
            raise ValueError("Secret not found or not in string format")
