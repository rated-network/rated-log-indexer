from typing import Optional, Any

import structlog
from rated_parser import LogParser
from rated_parser.core.payloads import RawTextLogPatternPayload, JsonLogPatternPayload
from rated_parser import LogFormat as RatedParserLogFormat

from src.config.models.filters import FiltersYamlConfig
from src.indexers.filters.types import FilteredEvent, LogEntry

logger = structlog.getLogger(__name__)


class FilterManager:
    def __init__(self, filter_config: FiltersYamlConfig):
        self.log_parser = LogParser()
        self.filter_config = filter_config
        self._initialize_parser()

    def _initialize_parser(self):
        pattern = {
            "version": self.filter_config.version,
            "log_format": self.filter_config.log_format,
            "log_example": self.filter_config.log_example,
            "fields": [field.dict() for field in self.filter_config.fields],
        }

        if self.filter_config.log_format == RatedParserLogFormat.RAW_TEXT:
            pattern_payload = RawTextLogPatternPayload(**pattern)
        elif self.filter_config.log_format == RatedParserLogFormat.JSON:
            pattern_payload = JsonLogPatternPayload(**pattern)
        else:
            raise ValueError(f"Unsupported log format: {self.filter_config.log_format}")

        self.log_parser.add_pattern(pattern_payload.dict())

    def parse_and_filter_log(self, log_entry: LogEntry) -> Optional[FilteredEvent]:
        """
        Returns parsed fields dictionary from the log entry if the log entry is successfully parsed and filtered.
        """
        try:
            parsed_log = self.log_parser.parse_log(
                log_entry.content, version=self.filter_config.version
            )
            # if not customer_id or values is {} or none then return None
            return FilteredEvent(
                log_id=log_entry.log_id,
                event_timestamp=log_entry.event_timestamp,
                # TODO: improve this upstream to force customer_id value in config filters.
                customer_id=parsed_log.parsed_fields.get(
                    "customer_id", "MISSING_CUSTOMER_ID"
                ),
                values=parsed_log.parsed_fields,
            )

        except Exception as e:
            logger.error("Parsing error", log_content=log_entry.content, error=str(e))
            return None

    def parse_and_filter_metrics(self, metrics_entry: Any) -> Optional[FilteredEvent]:
        # TODO: Implement metrics parsing and filtering
        raise NotImplementedError("Metrics parsing and filtering not yet implemented")
