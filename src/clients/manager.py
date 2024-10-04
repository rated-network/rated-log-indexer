import uuid
from typing import Dict, Union, Optional, Type

from pydantic import StrictStr

from src.clients.postgres import PostgresClient
from src.config.models.inputs.sql import SqlConfig
from src.clients.cloudwatch import CloudwatchClient
from src.clients.datadog import DatadogClient
from src.config.models.inputs.cloudwatch import CloudwatchConfig
from src.config.models.inputs.datadog import DatadogConfig
from src.config.models.inputs.input import IntegrationTypes

ClientType = Union[CloudwatchClient, DatadogClient, PostgresClient]
ConfigType = Union[CloudwatchConfig, DatadogConfig, SqlConfig]


class ClientManager:
    def __init__(self):
        self.clients: Dict[str, ClientType] = {}
        self.client_factories: Dict[IntegrationTypes, Type[ClientType]] = {
            IntegrationTypes.CLOUDWATCH: CloudwatchClient,
            IntegrationTypes.DATADOG: DatadogClient,
            IntegrationTypes.SQL: PostgresClient,
        }
        self.config_types: Dict[IntegrationTypes, Type[ConfigType]] = {
            IntegrationTypes.CLOUDWATCH: CloudwatchConfig,
            IntegrationTypes.DATADOG: DatadogConfig,
            IntegrationTypes.SQL: SqlConfig,
        }

    def add_client(
        self,
        integration_type: IntegrationTypes,
        config: ConfigType,
    ) -> StrictStr:
        if integration_type not in self.client_factories:
            raise ValueError(f"Unsupported integration type: {integration_type}")

        if not isinstance(config, self.config_types[integration_type]):
            raise TypeError(f"Invalid config type for {integration_type}")

        client_id = str(uuid.uuid4())
        client_class = self.client_factories[integration_type]
        self.clients[client_id] = client_class(config)  # type: ignore
        return client_id

    def get_client(self, client_id: str) -> Optional[ClientType]:
        return self.clients.get(client_id)

    def register_integration(
        self,
        integration_type: IntegrationTypes,
        client_class: Type[ClientType],
        config_class: Type[ConfigType],
    ) -> None:
        self.client_factories[integration_type] = client_class
        self.config_types[integration_type] = config_class
