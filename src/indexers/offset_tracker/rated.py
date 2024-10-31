from datetime import UTC, datetime
from pydantic import StrictStr
import structlog

from src.clients.slaos import SlaosClient
from src.config.models.offset import OffsetYamlConfig, OffsetSlaosYamlConfig
from src.config.models.output import RatedOutputConfig
from src.indexers.offset_tracker.base import OffsetTracker

logger = structlog.get_logger(__name__)


class RatedAPIOffsetTracker(OffsetTracker):
    _current_offset: int

    def __init__(self, config: OffsetYamlConfig, integration_prefix: StrictStr):
        super().__init__(config=config, integration_prefix=integration_prefix)
        slaos_config: OffsetSlaosYamlConfig = config.slaos  # type: ignore[assignment]
        client_config = RatedOutputConfig(**slaos_config.model_dump())
        self.client = SlaosClient(client_config)
        self.initialise_offset()

    def get_current_offset(self) -> int:  # type: ignore[return-value]
        return self._current_offset

    def update_offset(self, offset: int) -> None:
        # Just keep track of offset internally, no need to send it to the API.
        self._current_offset = offset

    def initialise_offset(self) -> None:
        if self.config.override_start_from:
            offset = self.config.start_from
            logger.info(
                "`override_start_from` detected - using configured `start_from`",
                offset=offset,
            )
        else:
            logger.info("Getting start date from API...")
            api_timestamp = self.get_offset_from_api()
            if api_timestamp is not None:
                from_epoch = api_timestamp - datetime(1970, 1, 1, tzinfo=UTC)
                offset = int(from_epoch.total_seconds() * 1000)
                logger.info(
                    "Success - using starting point from API",
                    api_timestamp=api_timestamp,
                    offset=offset,
                )
            else:
                offset = self.config.start_from
                logger.info(
                    "No start date returned from API - using configured `start_from`",
                    offset=offset,
                )

        self._current_offset = offset

    def get_offset_from_api(self) -> datetime | None:
        key = self.config.slaos.datastream_key  # type: ignore[union-attr]
        return self.client.get_latest_ingest_timestamp(key)
