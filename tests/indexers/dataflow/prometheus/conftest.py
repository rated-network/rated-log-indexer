import json
import shutil
import time
from pathlib import Path
from typing import Generator

import docker
import requests
import responses
import re
import pytest
from docker.models.networks import Network
from pydantic_core import Url
from pytest_httpx import HTTPXMock
from testcontainers.core.container import DockerContainer  # type: ignore

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
from dotenv import load_dotenv

PROMETHEUS_PORT = 9090
METRICS_GENERATOR_APP_PORT = 8000

load_dotenv()


def wait_for_http_service(url: str, timeout: int = 10) -> bool:
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(url)
            response.raise_for_status()
            return True
        except requests.RequestException:
            time.sleep(1)
    return False


def get_container_url(container: DockerContainer, port: int) -> str:
    host = container.get_container_host_ip()
    exposed_port = container.get_exposed_port(port)
    return f"http://{host}:{exposed_port}"


@pytest.fixture(scope="session")
def docker_client() -> docker.DockerClient:
    return docker.from_env()


@pytest.fixture(scope="session")
def docker_network(
    docker_client: docker.DockerClient,
) -> Generator[Network, None, None]:
    yield (network := docker_client.networks.create("test_prometheus_network"))
    network.remove()


@pytest.fixture(scope="session")
def fake_app_url(fake_app_container: DockerContainer):
    return get_container_url(fake_app_container, METRICS_GENERATOR_APP_PORT)


@pytest.fixture(scope="session")
def fake_app_container(
    docker_client: docker.DockerClient, docker_network: Network
) -> Generator[DockerContainer, None, None]:

    app_tag = "fake-app:latest"
    docker_client.images.build(
        path="tests/indexers/dataflow/prometheus",
        dockerfile="Dockerfile.fake_app",
        tag=app_tag,
        rm=True,
    )

    container = DockerContainer(app_tag)
    container.with_exposed_ports(METRICS_GENERATOR_APP_PORT)
    container.with_network(docker_network)
    container.with_name("fake-app")
    container.with_env("HOSTNAME", "fake-app")
    container.start()
    url = get_container_url(container, METRICS_GENERATOR_APP_PORT)

    if not wait_for_http_service(f"{url}/metrics"):
        raise RuntimeError("Fake app failed to start")

    container_ip = container.get_container_host_ip()
    print(f"Fake app container IP in network: {container_ip}")

    yield container
    container.stop()


@pytest.fixture(scope="session")
def prometheus_container(
    docker_network: Network, fake_app_container: DockerContainer
) -> Generator[DockerContainer, None, None]:
    config_dir = Path("tmp_prometheus_config")
    config_dir.mkdir(exist_ok=True)

    prometheus_config = f"""
global:
  scrape_interval: 3s
  evaluation_interval: 3s
scrape_configs:
  - job_name: 'fake_app'
    static_configs:
      - targets: ['fake-app:{METRICS_GENERATOR_APP_PORT}']
    metrics_path: '/metrics'
"""
    config_path = config_dir / "prometheus.yml"
    with open(config_path, "w") as f:
        f.write(prometheus_config)

    container = DockerContainer("prom/prometheus:latest")
    container.with_exposed_ports(PROMETHEUS_PORT)
    container.with_command("--config.file=/etc/prometheus/prometheus.yml")
    container.with_network(docker_network)
    container.with_name("prometheus")
    container.with_volume_mapping(
        host=str(config_path.absolute()),
        container="/etc/prometheus/prometheus.yml",
        mode="ro",
    )

    container.with_env("HOSTNAME", "prometheus")
    container.start()
    url = get_container_url(container, PROMETHEUS_PORT)

    if not wait_for_http_service(url):
        raise RuntimeError("Prometheus failed to start")

    def check_prometheus_target():
        try:
            response = requests.get(f"{url}/api/v1/targets")
            targets = response.json()
            active_targets = targets.get("data", {}).get("activeTargets", [])
            for target in active_targets:
                print(
                    f"Target state: {target.get('health')} - {target.get('labels', {}).get('instance')}"
                )
                if target.get("health") == "up":
                    return True
            return False
        except Exception as e:
            print(f"Error checking Prometheus target: {e}")
            return False

    start_time = time.time()
    while time.time() - start_time < 10:
        if check_prometheus_target():
            break
        print("Waiting for Prometheus target to become healthy...")
        time.sleep(2)
    else:
        print("Warning: Prometheus target did not become healthy within timeout")

    yield container
    container.stop()
    shutil.rmtree(config_dir)


@pytest.fixture(scope="session")
def prometheus_url(prometheus_container: DockerContainer) -> str:
    return get_container_url(prometheus_container, PROMETHEUS_PORT)


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
def prometheus_config(prometheus_url: str) -> PrometheusConfig:
    """Create a Prometheus configuration."""
    print("Prometheus URL", prometheus_url)
    return PrometheusConfig(
        base_url=Url(prometheus_url),
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
        timeout=6.5,
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
