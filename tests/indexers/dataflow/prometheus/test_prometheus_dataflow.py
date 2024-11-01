import json
import time
import subprocess
from bytewax.testing import run_main, TestingSource
from pydantic_core import Url
from pytest_httpx import HTTPXMock

from src.config.models.inputs.input import (
    InputYamlConfig,
    InputTypes,
    IntegrationTypes,
)
from src.config.models.output import (
    RatedOutputConfig,
    OutputTypes,
)
from src.indexers.filters.manager import FilterManager
from src.indexers.sources.rated import TimeRange
from src.indexers.dataflow import build_dataflow, fetch_metrics
from src.indexers.sinks.rated import build_http_sink


class TestPrometheusDataflow:
    def test_prometheus_dataflow_integration(
        self,
        fake_app_process: subprocess.Popen,
        prometheus_process: subprocess.Popen,
        input_config: InputYamlConfig,
        output_config: RatedOutputConfig,
        httpx_mock: HTTPXMock,
    ):
        """Test the Prometheus dataflow with real services."""
        time.sleep(15)

        current_time = int(time.time() * 1000)
        mock_input = TestingSource(
            [
                TimeRange(
                    start_time=current_time - 300000,  # 5 minutes ago
                    end_time=current_time,
                )
            ]
        )

        filter_manager = FilterManager(None, "prometheus_test", InputTypes.METRICS)

        inputs = [
            (
                IntegrationTypes.PROMETHEUS,
                InputTypes.METRICS,
                input_config.prometheus,
                mock_input,
                fetch_metrics,
                filter_manager.parse_and_filter_metrics,
                "prometheus_test",
            )
        ]

        flow = build_dataflow(
            inputs,  # type: ignore
            OutputTypes.RATED,
            lambda prefix: build_http_sink(output_config, slaos_key=prefix),
        )

        run_main(flow)

        requests = httpx_mock.get_requests()
        assert len(requests) > 0, "Should have received requests"

        for request in requests:
            data = json.loads(request.content)
            assert len(data) > 0, "Should have received metrics data"

            for item in data:
                assert "organization_id" in item
                assert "timestamp" in item
                assert "values" in item
                assert isinstance(item["values"], dict)

                values = item["values"]
                metric_name = next(iter(values.keys()))

                if metric_name == "test_endpoint_latency_rate":
                    assert (
                        item["organization_id"] == "FALLBACK_ORG_ID"
                    ), "Organization ID should be FALLBACK_ORG_ID in `test_endpoint_latency_rate`"
                else:
                    assert item["organization_id"] in [
                        "user123",
                        "user456",
                        "user789",
                    ], "Unexpected organization ID in metrics"

                assert metric_name in [
                    "test_requests",
                    "test_latency_p95",
                    "test_latency_rate",
                    "test_endpoint_latency_rate",
                ], "Unexpected metric name"

                if "labels" in values:
                    labels = values["labels"]
                    assert "endpoint" in labels, "All metrics should have endpoint"
                    assert labels["endpoint"] in ["/api/test1", "/api/test2"]

                    if metric_name == "test_requests":
                        assert "region" in labels, "test_requests should have region"
                        assert labels["region"] in ["us-east1", "us-west1"]

                value = next(iter(values.values()))
                assert isinstance(value, (int, float)), "Values should be numeric"

    def test_prometheus_dataflow_mock(
        self,
        input_config: InputYamlConfig,
        output_config: RatedOutputConfig,
        mock_prometheus_responses,
        httpx_mock: HTTPXMock,
    ):
        """Test the Prometheus dataflow with mocked responses."""
        input_config.prometheus.base_url = Url("http://mock-prometheus:9090")  # type: ignore
        current_time = int(time.time())
        mock_input = TestingSource(
            [
                TimeRange(
                    start_time=(current_time - 30) * 1000,
                    end_time=(current_time - 15) * 1000,
                )
            ]
        )

        with mock_prometheus_responses as rsps:
            filter_manager = FilterManager(None, "prometheus_test", InputTypes.METRICS)

            inputs = [
                (
                    IntegrationTypes.PROMETHEUS,
                    InputTypes.METRICS,
                    input_config.prometheus,
                    mock_input,
                    fetch_metrics,
                    filter_manager.parse_and_filter_metrics,
                    "prometheus_test",
                )
            ]

            flow = build_dataflow(
                inputs,  # type: ignore
                OutputTypes.RATED,
                lambda prefix: build_http_sink(output_config, slaos_key=prefix),
            )

            run_main(flow)

            prometheus_requests = [req for req in rsps.calls]
            print(prometheus_requests)
            assert (
                len(prometheus_requests) > 0
            ), "Should have made requests to Prometheus"

            ingestion_requests = httpx_mock.get_requests(
                url=f"{output_config.ingestion_url}/{output_config.ingestion_id}/{output_config.ingestion_key}"
            )
            assert (
                len(ingestion_requests) > 0
            ), "Should have made requests to ingestion endpoint"

            for request in ingestion_requests:
                sent_data = json.loads(request.content)

                for item in sent_data:
                    assert set(item.keys()) == {
                        "organization_id",
                        "timestamp",
                        "values",
                        "key",
                        "idempotency_key",
                    }
                    assert "values" in item
                    assert isinstance(item["values"], dict)

                    metric_name = next(iter(item["values"].keys()))
                    if metric_name == "test_endpoint_latency_rate":
                        assert item["organization_id"] == "FALLBACK_ORG_ID"
                    else:
                        assert item["organization_id"] in [
                            "mock_user1",
                            "mock_user2",
                        ], f"Unexpected organization_id: {item['organization_id']} for metric: {metric_name}"

                    assert metric_name in [
                        "test_requests",
                        "test_latency_p95",
                        "test_latency_rate",
                        "test_endpoint_latency_rate",
                    ]
                    values = item["values"]
                    if "labels" in values:
                        labels = values["labels"]
                        assert "endpoint" in labels
                        assert labels["endpoint"] in ["/api/test1", "/api/test2"]

                        if metric_name == "test_requests":
                            assert "region" in labels
                            assert labels["region"] in ["us-east1", "us-west1"]
