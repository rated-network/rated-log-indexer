import uuid
from typing import Dict, Union, Optional

from pydantic import StrictStr

from src.clients.cloudwatch import CloudwatchClient
from src.clients.datadog import DatadogClient
from src.config.models.inputs.cloudwatch import CloudwatchConfig
from src.config.models.inputs.datadog import DatadogConfig
from src.config.models.inputs.input import IntegrationTypes


class ClientManager:
    def __init__(self):
        self.clients: Dict[str, Union[CloudwatchClient, DatadogClient]] = {}

    def add_client(
        self,
        integration_type: IntegrationTypes,
        config: Union[CloudwatchConfig, DatadogConfig],
    ) -> StrictStr:
        client_id = str(uuid.uuid4())
        if integration_type == IntegrationTypes.CLOUDWATCH and isinstance(
            config, CloudwatchConfig
        ):
            self.clients[client_id] = CloudwatchClient(config)
        elif integration_type == IntegrationTypes.DATADOG and isinstance(
            config, DatadogConfig
        ):
            self.clients[client_id] = DatadogClient(config)
        else:
            raise ValueError(f"Unsupported integration type: {integration_type}")
        return client_id

    def get_client(
        self, client_id: str
    ) -> Optional[Union[CloudwatchClient, DatadogClient]]:
        return self.clients.get(client_id)
