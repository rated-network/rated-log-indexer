from typing import Optional

import structlog
from rated_parser import LogParser  # type: ignore
from rated_parser.payloads.inputs import RawTextLogPatternPayload, JsonLogPatternPayload, LogFormat as RatedParserLogFormat  # type: ignore

from src.config.models.inputs.input import InputTypes
from src.config.models.filters import FiltersYamlConfig
from src.indexers.filters.types import (
    FilteredEvent,
    LogEntry,
    MetricEntry,
    generate_idempotency_key,
)

logger = structlog.getLogger(__name__)


class FilterManager:
    def __init__(
        self,
        filter_config: Optional[FiltersYamlConfig],
        integration_prefix: str,
        input_type: InputTypes,
    ):
        self.log_parser = LogParser()
        self.input_type = input_type
        self.filter_config = filter_config
        self.integration_prefix = integration_prefix
        self._initialize_parser()

    def _initialize_parser(self):
        if self.input_type == InputTypes.METRICS:
            """No need to add pattern for metrics"""
            return
        if not self.filter_config:
            raise ValueError("FiltersYamlConfig is required for logs input type")
        pattern = {
            "version": self.filter_config.version,
            "log_format": self.filter_config.log_format,
            "log_example": self.filter_config.log_example,
            "fields": [field.model_dump() for field in self.filter_config.fields],
        }

        if self.filter_config.log_format == RatedParserLogFormat.RAW_TEXT:
            pattern_payload = RawTextLogPatternPayload(**pattern)
        elif self.filter_config.log_format == RatedParserLogFormat.JSON:
            pattern_payload = JsonLogPatternPayload(**pattern)
        else:
            raise ValueError(f"Unsupported log format: {self.filter_config.log_format}")

        self.log_parser.add_pattern(pattern_payload.model_dump())

    def parse_and_filter_log(self, log_entry: LogEntry) -> Optional[FilteredEvent]:
        """
        Returns parsed fields dictionary from the log entry if the log entry is successfully parsed and filtered.
        """
        try:
            parsed_log = self.log_parser.parse_log(
                log_entry.content, version=self.filter_config.version  # type: ignore
            )
            fields = parsed_log.parsed_fields
            if not fields or not fields.get("customer_id"):
                return None

            return FilteredEvent(
                integration_prefix=self.integration_prefix,
                idempotency_key=log_entry.log_id,
                event_timestamp=log_entry.event_timestamp,
                customer_id=parsed_log.parsed_fields.get(
                    "customer_id", "MISSING_CUSTOMER_ID"
                ),
                values=parsed_log.parsed_fields,
            )

        except Exception as e:
            logger.error("Parsing error", log_content=log_entry.content, error=str(e))
            return None

    def parse_and_filter_metrics(
        self, metrics_entry: MetricEntry
    ) -> Optional[FilteredEvent]:
        """
        Returns parsed fields dictionary from the metrics entry if the metrics entry is successfully parsed and filtered.
        """
        try:
            idempotency_key = generate_idempotency_key(
                event_timestamp=metrics_entry.event_timestamp,
                customer_id=metrics_entry.customer_id,
                values={metrics_entry.metric_name: metrics_entry.value},
            )

            return FilteredEvent(
                integration_prefix=self.integration_prefix,
                idempotency_key=idempotency_key,
                event_timestamp=metrics_entry.event_timestamp,
                customer_id=metrics_entry.customer_id,
                values={metrics_entry.metric_name: metrics_entry.value},
            )

        except Exception as e:
            logger.error("Parsing error", metric_content=metrics_entry, error=str(e))
            return None
