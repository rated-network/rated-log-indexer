import structlog

from src.config.models.sentry import initialize_sentry
from src.config.manager import ConfigurationManager
from src.indexers.dataflow import dataflow

logger = structlog.get_logger(__name__)


def main():
    config = ConfigurationManager.load_config()
    if not config.secrets.use_secrets_manager:
        logger.warning(
            "Secrets manager is disabled, its use is encouraged in production environments"
        )

    if config.sentry:
        initialize_sentry(config.sentry)

    flow = dataflow(config)
    return flow


if __name__ == "__main__":
    main()
