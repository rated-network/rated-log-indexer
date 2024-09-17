import pytest
from pydantic import ValidationError

from src.config.models.output import RatedOutputConfig


def test_ingestion_url_valid():
    config = RatedOutputConfig(
        ingestion_id="test-id",
        ingestion_key="test-key",
        ingestion_url="https://example.com/v1/ingest",
    )
    assert config.ingestion_url == "https://example.com/v1/ingest"


def test_ingestion_url_with_trailing_slash():
    config = RatedOutputConfig(
        ingestion_id="test-id",
        ingestion_key="test-key",
        ingestion_url="https://example.com/v1/ingest/",
    )
    assert config.ingestion_url == "https://example.com/v1/ingest"


def test_ingestion_url_invalid():
    with pytest.raises(
        ValidationError,
        match='ingestion_url must end with v{version_number}/ingest or start with "secret:"',
    ):
        RatedOutputConfig(
            ingestion_id="test-id",
            ingestion_key="test-key",
            ingestion_url="https://example.com/invalid",
        )


def test_ingestion_url_secret():
    config = RatedOutputConfig(
        ingestion_id="test-id",
        ingestion_key="test-key",
        ingestion_url="secret:my-secret-url",
    )
    assert config.ingestion_url == "secret:my-secret-url"


def test_ingestion_url_http():
    config = RatedOutputConfig(
        ingestion_id="test-id",
        ingestion_key="test-key",
        ingestion_url="http://example.com/v2/ingest",
    )
    assert config.ingestion_url == "http://example.com/v2/ingest"
