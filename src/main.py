import structlog

from src.config.manager import ConfigurationManager
from src.indexers.dataflow import dataflow

logger = structlog.get_logger(__name__)


def main():
    config = ConfigurationManager.load_config()
    if not config.secrets.use_secrets_manager:
        logger.warning(
            "Secrets manager is disabled, its use is encouraged in production environments"
        )

    flow = dataflow(config)
    return flow


if __name__ == "__main__":
    main()
