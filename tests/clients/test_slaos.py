from datetime import datetime
import httpx
import pytest
from pytest_httpx import HTTPXMock
import stamina

from src.clients import slaos
from src.config.models.output import RatedOutputConfig

INGESTION_ID = "some-uuid"
INGESTION_KEY = "secret-key"
INGESTION_URL = "http://localhost:8000/v1/ingest"


@pytest.fixture
def slaos_client_config() -> RatedOutputConfig:
    return RatedOutputConfig(
        ingestion_id="some-uuid",
        ingestion_key="secret-key",
        ingestion_url="http://localhost:8000/v1/ingest",
    )


def test_slaos_client_full_ingest_url(slaos_client_config: RatedOutputConfig):
    client = slaos.SlaosClient(slaos_client_config)

    assert (
        client.full_ingest_url == "http://localhost:8000/v1/ingest/some-uuid/secret-key"
    )


def test_slaos_client_get_latest_ingest_timestamp_ok(
    httpx_mock: HTTPXMock, slaos_client_config: RatedOutputConfig
):
    httpx_mock.add_response(
        url=httpx.URL(
            f"{INGESTION_URL}/{INGESTION_ID}/{INGESTION_KEY}/indexed-slis",
            params={"limit": 1, "key": "datastream-key"},
        ),
        method="GET",
        json=[
            {
                "customer_id": "your-customer",
                "key": "datastream-key",
                "vendor_id": "your-org-id",
                "timestamp": "2024-09-09T22:00:23+00:00",
                "report_timestamp": "2024-09-09T22:58:34.739860+00:00",
                "doc_type": "sli",
                "ingestion_timestamp": "2024-09-09T22:58:35.513191+00:00",
                "avg_uptime": 1.0,
                "hour": 33082,
            }
        ],
    )
    client = slaos.SlaosClient(slaos_client_config)
    timestamp = client.get_latest_ingest_timestamp("datastream-key")

    assert timestamp == datetime.fromisoformat("2024-09-09T22:00:23+00:00")


def test_slaos_client_get_latest_ingest_timestamp_unknown_datastream(
    httpx_mock: HTTPXMock, slaos_client_config: RatedOutputConfig
):
    httpx_mock.add_response(
        url=httpx.URL(
            f"{INGESTION_URL}/{INGESTION_ID}/{INGESTION_KEY}/indexed-slis",
            params={"limit": 1, "key": "datastream-key"},
        ),
        method="GET",
        json=[],
        status_code=httpx.codes.OK.value,
    )
    client = slaos.SlaosClient(slaos_client_config)
    timestamp = client.get_latest_ingest_timestamp("datastream-key")

    assert timestamp is None


def test_slaos_client_get_latest_ingest_timestamp_unknown_ingest_params(
    httpx_mock: HTTPXMock, slaos_client_config: RatedOutputConfig
):
    stamina.set_testing(True)
    httpx_mock.add_response(
        url=httpx.URL(
            f"{INGESTION_URL}/{INGESTION_ID}/{INGESTION_KEY}/indexed-slis",
            params={"limit": 1, "key": "datastream-key"},
        ),
        method="GET",
        json={"detail": "Vendor not found"},
        status_code=httpx.codes.NOT_FOUND.value,
    )
    client = slaos.SlaosClient(slaos_client_config)

    with pytest.raises(httpx.HTTPStatusError):
        client.get_latest_ingest_timestamp("datastream-key")
