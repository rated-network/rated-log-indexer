import uuid
from typing import Dict, Union, Optional, TypeAlias

from pydantic import StrictStr

from src.clients.prometheus import PrometheusClientWrapper
from src.config.models.inputs.prometheus import PrometheusConfig
from src.clients.cloudwatch import CloudwatchClient
from src.clients.datadog import DatadogClient
from src.config.models.inputs.cloudwatch import CloudwatchConfig
from src.config.models.inputs.datadog import DatadogConfig
from src.config.models.inputs.input import IntegrationTypes

ClientConfigTypes: TypeAlias = Union[DatadogConfig, CloudwatchConfig, PrometheusConfig]
ClientTypes: TypeAlias = Union[CloudwatchClient, DatadogClient, PrometheusClientWrapper]


class ClientManager:
    def __init__(self):
        self.clients: Dict[StrictStr, ClientTypes] = {}
        self.client_factories = {
            IntegrationTypes.CLOUDWATCH: (CloudwatchClient, CloudwatchConfig),
            IntegrationTypes.DATADOG: (DatadogClient, DatadogConfig),
            IntegrationTypes.PROMETHEUS: (PrometheusClientWrapper, PrometheusConfig),
        }

    def add_client(
        self,
        integration_type: IntegrationTypes,
        config: ClientConfigTypes,
    ) -> StrictStr:
        client_id = str(uuid.uuid4())
        client_class, config_class = self.client_factories.get(
            integration_type, (None, None)
        )

        if client_class and isinstance(config, config_class):
            self.clients[client_id] = client_class(config)
            return client_id

        raise ValueError(f"Unsupported integration type: {integration_type}")

    def get_client(self, client_id: str) -> Optional[ClientTypes]:
        return self.clients.get(client_id)
