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
