import json
import time
import subprocess
from typing import Generator
import responses
import re
import pytest
from pydantic_core import Url
from pytest_httpx import HTTPXMock

from src.config.models.inputs.prometheus import (
    PrometheusConfig,
    PrometheusQueryConfig,
)
from src.config.models.inputs.input import (
    InputYamlConfig,
    InputTypes,
    IntegrationTypes,
)
from src.config.models.offset import (
    OffsetYamlConfig,
    OffsetRedisYamlConfig,
    OffsetTypes,
    StartFromTypes,
)
from src.config.models.output import (
    RatedOutputConfig,
)


@pytest.fixture
def mocked_ingestion_endpoint(httpx_mock: HTTPXMock) -> str:
    """Create a mocked ingestion endpoint."""
    endpoint = "https://ingest.test.rated.network/v1/ingest"
    httpx_mock.add_response(
        method="POST",
        url=f"{endpoint}/test_ingestion_id/test_ingestion_key",
        status_code=200,
        json={"status": "success"},
    )
    return endpoint


@pytest.fixture
def fake_app_process() -> Generator[subprocess.Popen, None, None]:
    """Start the fake metrics-generating app."""
    process = subprocess.Popen(
        ["python", "fake_app.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    # Wait for server to start
    time.sleep(2)
    yield process
    process.terminate()
    process.wait()


@pytest.fixture
def prometheus_process() -> Generator[subprocess.Popen, None, None]:
    """Start the Prometheus server."""
    process = subprocess.Popen(
        ["prometheus", "--config.file=prometheus.yml"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    time.sleep(5)
    yield process
    process.terminate()
    process.wait()


@pytest.fixture
def prometheus_config() -> PrometheusConfig:
    """Create a Prometheus configuration."""
    return PrometheusConfig(
        base_url=Url("http://localhost:9090"),
        queries=[
            PrometheusQueryConfig(
                query="test_requests_total",
                step={"value": 15, "unit": "s"},
                organization_identifier="user_id",
                fallback_org_id=None,
                slaos_metric_name="test_requests",
            ),
            PrometheusQueryConfig(
                query="histogram_quantile(0.95, sum(rate(test_latency_seconds_bucket[5m])) by (le, user_id, endpoint))",
                step={"value": 15, "unit": "s"},
                organization_identifier="user_id",
                fallback_org_id=None,
                slaos_metric_name="test_latency_p95",
            ),
            PrometheusQueryConfig(
                query="sum(rate(test_latency_seconds_count[5m])) by (user_id, endpoint)",
                step={"value": 15, "unit": "s"},
                organization_identifier="user_id",
                fallback_org_id=None,
                slaos_metric_name="test_latency_rate",
            ),
            PrometheusQueryConfig(
                query="sum(rate(test_latency_seconds_count[5m])) by (endpoint)",
                step={"value": 15, "unit": "s"},
                organization_identifier="user_id",
                fallback_org_id="FALLBACK_ORG_ID",
                slaos_metric_name="test_endpoint_latency_rate",
            ),
        ],
        max_retries=3,
        retry_backoff_factor=1.0,
        pool_connections=10,
        pool_maxsize=10,
        max_parallel_queries=2,
    )


@pytest.fixture
def output_config(mocked_ingestion_endpoint: str) -> RatedOutputConfig:
    """Create an output configuration."""
    return RatedOutputConfig(
        ingestion_id="test_ingestion_id",
        ingestion_key="test_ingestion_key",
        ingestion_url=mocked_ingestion_endpoint,
    )


@pytest.fixture
def input_config(prometheus_config: PrometheusConfig) -> InputYamlConfig:
    """Create an input configuration."""
    return InputYamlConfig(
        type=InputTypes.METRICS,
        integration=IntegrationTypes.PROMETHEUS,
        slaos_key="prometheus_test",
        prometheus=prometheus_config,
        offset=OffsetYamlConfig(
            type=OffsetTypes.REDIS,
            start_from=int(time.time() * 1000) - 300000,  # 5 minutes ago
            start_from_type=StartFromTypes.BIGINT,
            redis=OffsetRedisYamlConfig(
                host="localhost", port=6379, db=0, key="prometheus_test"
            ),
        ),
        filters=None,
    )


@pytest.fixture
def mock_prometheus_response_for_test_requests() -> dict:
    """Create a mock Prometheus response for test_requests query."""
    current_time = int(time.time())
    return {
        "status": "success",
        "data": {
            "resultType": "matrix",
            "result": [
                {
                    "metric": {
                        "__name__": "test_requests_total",
                        "endpoint": "/api/test1",
                        "user_id": "mock_user1",
                        "region": "us-east1",
                    },
                    "values": [
                        [current_time - 30, "1.0"],
                        [current_time - 15, "2.0"],
                    ],
                },
                {
                    "metric": {
                        "__name__": "test_requests_total",
                        "endpoint": "/api/test2",
                        "user_id": "mock_user2",
                        "region": "us-west1",
                    },
                    "values": [
                        [current_time - 30, "3.0"],
                        [current_time - 15, "4.0"],
                    ],
                },
            ],
        },
    }


@pytest.fixture
def mock_prometheus_response_for_latency_p95() -> dict:
    """Create a mock Prometheus response for latency p95 query."""
    current_time = int(time.time())
    return {
        "status": "success",
        "data": {
            "resultType": "matrix",
            "result": [
                {
                    "metric": {
                        "endpoint": "/api/test1",
                        "user_id": "mock_user1",
                    },
                    "values": [
                        [current_time - 30, "0.095"],
                        [current_time - 15, "0.098"],
                    ],
                },
                {
                    "metric": {
                        "endpoint": "/api/test2",
                        "user_id": "mock_user2",
                    },
                    "values": [
                        [current_time - 30, "0.085"],
                        [current_time - 15, "0.088"],
                    ],
                },
            ],
        },
    }


@pytest.fixture
def mock_prometheus_response_for_latency_rate() -> dict:
    """Create a mock Prometheus response for latency rate query."""
    current_time = int(time.time())
    return {
        "status": "success",
        "data": {
            "resultType": "matrix",
            "result": [
                {
                    "metric": {
                        "endpoint": "/api/test1",
                        "user_id": "mock_user1",
                    },
                    "values": [[current_time - 30, "10"], [current_time - 15, "12"]],
                },
                {
                    "metric": {
                        "endpoint": "/api/test2",
                        "user_id": "mock_user2",
                    },
                    "values": [[current_time - 30, "8"], [current_time - 15, "9"]],
                },
            ],
        },
    }


@pytest.fixture
def mock_prometheus_response_for_endpoint_latency_rate() -> dict:
    """Create a mock Prometheus response for endpoint latency rate query."""
    current_time = int(time.time())
    return {
        "status": "success",
        "data": {
            "resultType": "matrix",
            "result": [
                {
                    "metric": {"endpoint": "/api/test1"},
                    "values": [[current_time - 30, "18"], [current_time - 15, "21"]],
                },
                {
                    "metric": {"endpoint": "/api/test2"},
                    "values": [[current_time - 30, "15"], [current_time - 15, "17"]],
                },
            ],
        },
    }


@pytest.fixture
def mock_prometheus_responses(
    mock_prometheus_response_for_test_requests: dict,
    mock_prometheus_response_for_latency_p95: dict,
    mock_prometheus_response_for_latency_rate: dict,
    mock_prometheus_response_for_endpoint_latency_rate: dict,
) -> Generator[responses.RequestsMock, None, None]:
    """Set up mock responses for all Prometheus endpoints."""
    with responses.RequestsMock() as rsps:
        # Mock the metadata endpoint
        rsps.add(
            responses.GET,
            "http://mock-prometheus:9090/api/v1/metadata",
            json={"status": "success", "data": {}},
            status=200,
        )

        def query_range_callback(request):
            query = request.params.get("query", "")
            if "test_requests_total" in query:
                return (200, {}, json.dumps(mock_prometheus_response_for_test_requests))
            elif "histogram_quantile" in query:
                return (200, {}, json.dumps(mock_prometheus_response_for_latency_p95))
            elif (
                "test_latency_seconds_count" in query
                and "by (user_id, endpoint)" in query
            ):
                return (200, {}, json.dumps(mock_prometheus_response_for_latency_rate))
            elif "test_latency_seconds_count" in query and "by (endpoint)" in query:
                return (
                    200,
                    {},
                    json.dumps(mock_prometheus_response_for_endpoint_latency_rate),
                )
            return (
                200,
                {},
                json.dumps(
                    {
                        "status": "success",
                        "data": {"resultType": "matrix", "result": []},
                    }
                ),
            )

        rsps.add_callback(
            responses.GET,
            re.compile(r"http://mock-prometheus:9090/api/v1/query_range.*"),
            callback=query_range_callback,
        )

        yield rsps
