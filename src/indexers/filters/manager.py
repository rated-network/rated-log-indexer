import re
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
        slaos_key: str,
        input_type: InputTypes,
    ):
        self.log_parser = LogParser()
        self.input_type = input_type
        self.filter_config = filter_config
        self.slaos_key = slaos_key
        self._initialize_parser()

    def _initialize_parser(self):
        if self.input_type == InputTypes.METRICS and not self.filter_config:
            """Parser is optional for Metrics input type"""
            return

        if self.input_type == InputTypes.LOGS and not self.filter_config:
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

    @staticmethod
    def _replace_special_characters(input_string: str) -> str:
        """
        Replace all special characters in the input string with underscores,
        except for forward slashes (/).
        """
        return re.sub(r"[^\w/]", "_", input_string, flags=re.UNICODE)

    def parse_and_filter_log(self, log_entry: LogEntry) -> Optional[FilteredEvent]:
        """
        Returns parsed fields dictionary from the log entry if the log entry is successfully parsed and filtered.
        """
        try:
            parsed_log = self.log_parser.parse_log(
                log_entry.content, version=self.filter_config.version  # type: ignore
            )
            fields = parsed_log.parsed_fields
            if not fields or not fields.get("organization_id"):
                logger.warning(
                    "Organization ID is missing, please update the filter logic to include `organization_id`",
                    parsed_fields=parsed_log.parsed_fields,
                    log_content=log_entry.content,
                )
                return None

            validated_fields = {
                self._replace_special_characters(k): v
                for k, v in parsed_log.parsed_fields.items()
            }

            return FilteredEvent(
                slaos_key=self.slaos_key,
                idempotency_key=log_entry.log_id,
                event_timestamp=log_entry.event_timestamp,
                organization_id=parsed_log.parsed_fields.get("organization_id"),
                values=validated_fields,
            )

        except Exception as e:
            logger.error(
                "Log parsing error", log_content=log_entry.content, error=str(e)
            )
            return None

    def parse_and_filter_metrics(
        self, metrics_entry: MetricEntry
    ) -> Optional[FilteredEvent]:
        """
        Returns parsed fields dictionary from the metrics entry if the metrics entry is successfully parsed and filtered.
        """
        try:
            values = {
                self._replace_special_characters(
                    metrics_entry.metric_name
                ): metrics_entry.value
            }

            if metrics_entry.labels:
                parsed_metric = self.log_parser.parse_log(
                    metrics_entry.labels, version=self.filter_config.version  # type: ignore
                )

                if (
                    not parsed_metric.parsed_fields
                    or not parsed_metric.parsed_fields.get("organization_id")
                ):
                    return None

                values.update(
                    {
                        self._replace_special_characters(k): v
                        for k, v in parsed_metric.parsed_fields.items()
                    }
                )

            idempotency_key = generate_idempotency_key(
                event_timestamp=metrics_entry.event_timestamp,
                organization_id=metrics_entry.organization_id,
                values=values,
            )

            return FilteredEvent(
                slaos_key=self.slaos_key,
                idempotency_key=idempotency_key,
                event_timestamp=metrics_entry.event_timestamp,
                organization_id=metrics_entry.organization_id,
                values=values,
            )

        except Exception as e:
            logger.error(
                "Metric parsing error", metric_content=metrics_entry, error=str(e)
            )
            return None
