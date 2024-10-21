import pytest

from src.clients.cloudwatch import CloudwatchClient
from src.clients.datadog import DatadogClient
from src.clients.manager import ClientManager
from src.config.manager import RatedIndexerYamlConfig
from src.config.models.inputs.datadog import DatadogConfig, DatadogMetricsConfig
from src.config.models.inputs.input import IntegrationTypes


@pytest.fixture
def client_manager():
    return ClientManager()


def test_add_clients(client_manager, valid_config_dict):
    config = RatedIndexerYamlConfig(**valid_config_dict)
    cloudwatch_config = config.inputs[0].cloudwatch

    datadog_config = DatadogConfig(
        api_key="your_api_key",
        app_key="your_app_key",
        site="datadog.eu",
        metrics_config=DatadogMetricsConfig(
            metric_name="test.metric",
            interval=60,
            statistic="AVERAGE",
            organization_identifier="customer",
            metric_tag_data=[
                {"customer_value": "customer1", "tag_string": "customer:customer1"},  # type: ignore
                {"customer_value": "customer2", "tag_string": "customer:customer2"},  # type: ignore
            ],
        ),
    )

    cw_client_id = client_manager.add_client(
        IntegrationTypes.CLOUDWATCH, cloudwatch_config
    )
    cw_client = client_manager.get_client(cw_client_id)

    dd_client_id = client_manager.add_client(IntegrationTypes.DATADOG, datadog_config)
    dd_client = client_manager.get_client(dd_client_id)

    assert isinstance(cw_client, CloudwatchClient), "Expected a CloudwatchClient"
    assert cw_client.config == cloudwatch_config, "Client configuration does not match"
    assert isinstance(dd_client, DatadogClient), "Expected a DatadogClient"
    assert dd_client.config == datadog_config, "Client configuration does not match"
    assert len(client_manager.clients) == 2, "Expected 2 clients to be added"


def test_adding_invalid_config_raises_error(client_manager, valid_config_dict):
    config = RatedIndexerYamlConfig(**valid_config_dict)
    cloudwatch_config = config.inputs[0].cloudwatch

    with pytest.raises(ValueError) as excinfo:
        client_manager.add_client(IntegrationTypes.DATADOG, cloudwatch_config)
    assert "Unsupported integration type" in str(excinfo.value)
