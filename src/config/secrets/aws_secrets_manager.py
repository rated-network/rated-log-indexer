import boto3  # type: ignore
from botocore.exceptions import ClientError  # type: ignore
from pydantic import BaseModel, StrictStr

from src.config.secrets.manager import SecretManager


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

    def resolve_secret(self, secret_id: StrictStr) -> StrictStr:
        try:
            get_secret_value_response = self.secrets_client.get_secret_value(
                SecretId=secret_id
            )
        except ClientError as e:
            ## TODO: Improve error handling
            raise e

        if "SecretString" in get_secret_value_response:
            return get_secret_value_response["SecretString"]
        else:
            raise ValueError("Secret not found or not in string format")
