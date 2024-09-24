import sentry_sdk
import structlog
from pydantic import BaseModel, Field, StrictStr, confloat
from typing import Optional

logger = structlog.get_logger(__name__)


class SentryYamlConfig(BaseModel):
    dsn: StrictStr
    traces_sample_rate: confloat(ge=0, le=1) = Field(  # type: ignore
        default=1.0, description="Traces sample rate (0.0 to 1.0)"
    )
    profiles_sample_rate: confloat(ge=0, le=1) = Field(  # type: ignore
        default=1.0, description="Profiles sample rate (0.0 to 1.0)"
    )
    environment: StrictStr
    release: StrictStr
    ingestion_id: StrictStr
    description: Optional[StrictStr] = Field(
        default=None, description="Custom description for this Sentry configuration"
    )


def initialize_sentry(config: SentryYamlConfig):
    sentry_sdk.init(
        dsn=config.dsn,
        traces_sample_rate=config.traces_sample_rate,
        profiles_sample_rate=config.profiles_sample_rate,
        release=config.release,
        environment=config.environment,
    )
    sentry_sdk.set_tag("ingestion_id", config.ingestion_id)
    if config.description:
        sentry_sdk.set_tag("configuration_description", config.description)
        logger.info(
            "Sentry initialized for integration-indexers.",
            sentry_description=config.description,
        )
