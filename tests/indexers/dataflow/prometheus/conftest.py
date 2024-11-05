import json
import shutil
import socket
import time
from pathlib import Path
from typing import Generator, Tuple

import docker
import requests
import responses
import re
import pytest
from docker.errors import NotFound, BuildError
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

# PROMETHEUS_CONTAINER_NAME = "test_prometheus"
PROMETHEUS_PORT = 9090
METRICS_GENERATOR_APP_CONTAINER_NAME = "test_fake_app"
SHARED_NETWORK = "test_prometheus_network"


def is_port_in_use(port: int, host: str = "0.0.0.0") -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex((host, port)) == 0


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


@pytest.fixture(scope="session")
def docker_client() -> docker.DockerClient:
    return docker.from_env()


@pytest.fixture(autouse=True, scope="session")
def docker_cleanup(
    docker_client: docker.DockerClient, prometheus_container: DockerContainer
):
    def cleanup():
        prometheus_container._name
        # First stop and remove containers
        for name in [prometheus_container._name, METRICS_GENERATOR_APP_CONTAINER_NAME]:
            try:
                container = docker_client.containers.get(name)
                # Disconnect from all networks first
                container_networks = container.attrs["NetworkSettings"]["Networks"]
                for network_name in container_networks:
                    try:
                        network = docker_client.networks.get(network_name)
                        network.disconnect(container, force=True)
                    except (docker.errors.NotFound, docker.errors.APIError):
                        pass
                time.sleep(0.5)
                container.stop(timeout=1)
                container.remove(force=True)
            except docker.errors.NotFound:
                pass
            except docker.errors.APIError as e:
                print(f"Warning: Error handling container {name}: {e}")

    cleanup()
    yield
    cleanup()


@pytest.fixture(scope="session")
def docker_network(
    docker_client: docker.DockerClient,
) -> Generator[Network, None, None]:
    try:
        networks = docker_client.networks.list(names=[SHARED_NETWORK])
        for network in networks:
            try:
                containers = network.attrs.get("Containers", {})
                for container_id in containers:
                    try:
                        network.disconnect(container_id, force=True)
                        time.sleep(0.5)
                    except Exception:
                        pass
                network.remove()
            except Exception:
                pass
    except Exception:
        pass

    network = docker_client.networks.create(SHARED_NETWORK)
    yield network

    try:
        network.remove()
    except Exception:
        pass


@pytest.fixture(scope="session")
def fake_app(
    docker_client: docker.DockerClient, docker_network: Network
) -> Generator[Tuple[str, int], None, None]:
    host_port = 8000
    while is_port_in_use(host_port):
        host_port += 1

    try:
        docker_client.containers.get(METRICS_GENERATOR_APP_CONTAINER_NAME).remove(
            force=True
        )
    except NotFound:
        pass

    try:
        docker_client.images.build(
            path="tests/indexers/dataflow/prometheus",
            dockerfile="Dockerfile.fake_app",
            tag="fake-app:latest",
            rm=True,
        )
    except BuildError as e:
        print("Build error:", e)
        raise

    container = docker_client.containers.run(
        "fake-app:latest",
        name=METRICS_GENERATOR_APP_CONTAINER_NAME,
        detach=True,
        ports={"8000/tcp": host_port},
        network=docker_network.name,
    )

    if not wait_for_http_service(f"http://localhost:{host_port}/metrics"):
        raise RuntimeError("Fake app failed to start")

    yield METRICS_GENERATOR_APP_CONTAINER_NAME, host_port
    container.remove(force=True)


@pytest.fixture(scope="session")
def prometheus_container(
    docker_network: Network,
    fake_app: Tuple[str, int],
) -> Generator[DockerContainer, None, None]:
    fake_app_host, fake_app_port = fake_app

    config_dir = Path("tmp_prometheus_config")
    config_dir.mkdir(exist_ok=True)
    prometheus_config = f"""
    global:
      scrape_interval: 3s
      evaluation_interval: 3s
    scrape_configs:
      - job_name: 'fake_app'
        static_configs:
          - targets: ['{fake_app_host}:{fake_app_port}']
    """
    config_path = config_dir / "prometheus.yml"
    with open(config_path, "w") as f:
        f.write(prometheus_config)

    container = DockerContainer("prom/prometheus:latest")
    container.with_exposed_ports(PROMETHEUS_PORT)
    container.with_command("--config.file=/etc/prometheus/prometheus.yml")
    container.with_network(docker_network)
    container.with_volume_mapping(
        host=str(config_path.absolute()),
        container="/etc/prometheus/prometheus.yml",
        mode="ro",
    )
    container.start()
    url = make_prometheus_url(container)

    if not wait_for_http_service(url):
        raise RuntimeError("Prometheus failed to start")

    yield container

    container.stop()
    shutil.rmtree(config_dir)


def make_prometheus_url(prometheus_container: DockerContainer) -> str:
    host = prometheus_container.get_container_host_ip()
    port = prometheus_container.get_exposed_port(PROMETHEUS_PORT)
    return f"http://{host}:{port}"


@pytest.fixture
def prometheus_url(prometheus_container: DockerContainer) -> str:
    return make_prometheus_url(prometheus_container)


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
