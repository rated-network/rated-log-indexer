from unittest.mock import patch

from bytewax.testing import run_main, TestingSource
from pytest_httpx import HTTPXMock

from src.config.models.input import InputTypes
from src.config.models.output import OutputTypes
from src.indexers.sinks.rated import build_http_sink
from src.indexers.sources.cloudwatch import TimeRange
from src.config.manager import RatedIndexerYamlConfig
from src.indexers.dataflow import build_dataflow


@patch("src.indexers.dataflow.fetch_cloudwatch_logs")
@patch("src.config.manager.ConfigurationManager.load_config")
def test_dataflow(
    mock_config, mock_fetch_cloudwatch_logs, httpx_mock: HTTPXMock, valid_config_dict
):
    config = RatedIndexerYamlConfig(**valid_config_dict)
    mock_config.return_value = config

    sample_logs = [{"message": "log1"}, {"message": "log2"}]
    mock_fetch_cloudwatch_logs.return_value = iter(sample_logs)

    endpoint = "https://your_ingestion_url.com"
    httpx_mock.add_response(method="POST", url=endpoint, status_code=200)

    mock_input = TestingSource([TimeRange(start_time=1, end_time=2)])
    flow = build_dataflow(
        InputTypes.CLOUDWATCH,
        mock_input,
        mock_fetch_cloudwatch_logs,
        OutputTypes.RATED,
        build_http_sink(endpoint),
    )

    run_main(flow)

    mock_fetch_cloudwatch_logs.assert_called()
    requests = httpx_mock.get_requests()
    assert len(requests) == 2
