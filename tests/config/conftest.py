import logging

import pytest
import structlog


@pytest.fixture(autouse=True)
def setup_test_logging():
    """Configure structlog to use testing configuration."""
    structlog.configure(
        processors=[
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    handler = logging.StreamHandler()
    handler.setLevel(logging.WARNING)

    logger = structlog.get_logger()
    logger.addHandler(handler)

    yield

    logger.removeHandler(handler)


@pytest.fixture
def test_config():
    return {
        "inputs": [
            {
                "integration": "prometheus",
                "slaos_key": "test",
                "type": "metrics",
                "prometheus": {"base_url": "http://localhost:9090", "queries": []},
                "offset": {
                    "type": "slaos",
                    "override_start_from": False,
                    "start_from": 1730729435241,
                    "start_from_type": "bigint",
                    "slaos": {
                        "ingestion_id": "test",
                        "ingestion_key": "test",
                        "ingestion_url": "https://api.rated.co/v1/ingest",
                        "datastream_filter": {"key": "test", "customer_id": "test"},
                    },
                },
            }
        ],
        "output": {
            "type": "rated",
            "rated": {
                "ingestion_id": "test",
                "ingestion_key": "test",
                "ingestion_url": "https://api.rated.co/v1/ingest",
            },
        },
        "secrets": {"use_secrets_manager": False},
    }


@pytest.fixture
def config_paths(tmp_path):
    root_dir = tmp_path
    config_dir = root_dir / "config"
    config_dir.mkdir()
    return {
        "root_dir": root_dir,
        "new_config_path": config_dir / "rated-config.yaml",
        "old_config_path": root_dir / "rated-config.yaml",
    }
