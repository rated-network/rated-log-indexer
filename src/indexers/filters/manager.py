import re
from typing import Optional, Dict, Union

import structlog
from rated_parser import RatedParser  # type: ignore
from rated_parser.payloads.log_patterns import RawTextLogPattern, JsonLogPattern, LogFormat as RatedParserLogFormat  # type: ignore
from rated_parser.payloads.metric_patterns import MetricPattern  # type: ignore

from src.config.models.inputs.input import InputTypes
from src.config.models.filters import MetricFilterConfig, LogFilterConfig
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
        filter_config: Optional[Union[LogFilterConfig, MetricFilterConfig]],
        slaos_key: str,
        input_type: InputTypes,
    ):
        self.parser = RatedParser()
        self.input_type = input_type
        self.filter_config = filter_config
        self.slaos_key = slaos_key
        self._initialize_parser()

    def _initialize_parser(self):
        if self._should_skip_initialization():
            return

        self._validate_configuration()
        pattern = self._create_pattern()
        self._add_pattern_to_parser(pattern)

    def _should_skip_initialization(self) -> bool:
        return self.input_type == InputTypes.METRICS and not self.filter_config

    def _validate_configuration(self):
        if self.input_type == InputTypes.LOGS and not isinstance(
            self.filter_config, LogFilterConfig
        ):
            raise ValueError("Log input type requires LogFilterConfig")

        if (
            self.input_type == InputTypes.METRICS
            and self.filter_config
            and not isinstance(self.filter_config, MetricFilterConfig)
        ):
            raise ValueError("Metric input type requires MetricFilterConfig")

    def _create_pattern(self) -> Dict:
        if self.input_type == InputTypes.LOGS and isinstance(
            self.filter_config, LogFilterConfig
        ):
            return {
                "version": self.filter_config.version,
                "log_format": self.filter_config.log_format,
                "log_example": self.filter_config.log_example,
                "fields": [field.model_dump() for field in self.filter_config.fields],
            }
        elif self.input_type == InputTypes.METRICS and isinstance(
            self.filter_config, MetricFilterConfig
        ):
            return {
                "version": self.filter_config.version,
                "fields": [field.model_dump() for field in self.filter_config.fields],
            }
        else:
            raise ValueError(
                f"Invalid combination of input type ({self.input_type}) and "
                f"filter config type ({type(self.filter_config)})"
            )

    def _add_pattern_to_parser(self, pattern: Dict):
        if self.input_type == InputTypes.LOGS and isinstance(
            self.filter_config, LogFilterConfig
        ):
            if self.filter_config.log_format == RatedParserLogFormat.RAW_TEXT:
                pattern_payload = RawTextLogPattern(**pattern)
            elif self.filter_config.log_format == RatedParserLogFormat.JSON:
                pattern_payload = JsonLogPattern(**pattern)
            else:
                raise ValueError(
                    f"Unsupported log format: {self.filter_config.log_format}"
                )

            self.parser.add_log_pattern(pattern_payload.model_dump())
        elif self.input_type == InputTypes.METRICS and isinstance(
            self.filter_config, MetricFilterConfig
        ):
            pattern_payload = MetricPattern(**pattern)
            self.parser.add_metric_pattern(pattern_payload.model_dump())
        else:
            raise ValueError(
                f"Invalid combination of input type ({self.input_type}) and filter config {type(self.filter_config)}"
            )

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
        if not isinstance(self.filter_config, LogFilterConfig):
            raise ValueError("Cannot parse logs without LogFilterConfig")

        try:
            parsed_log = self.parser.parse_log(
                log_entry.content, version=self.filter_config.version
            )

            fields = parsed_log.parsed_fields
            if not fields or not fields.get("organization_id"):
                logger.warning(
                    "Organization ID is missing, please update the filter logic to include `organization_id`",
                    extra={
                        "parsed_fields": parsed_log.parsed_fields,
                        "log_content": log_entry.content,
                    },
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
                organization_id=parsed_log.parsed_fields["organization_id"],
                values=validated_fields,
            )

        except Exception as e:
            logger.error(
                "Log parsing error",
                extra={"log_content": log_entry.content, "error": str(e)},
            )
            return None

    def parse_and_filter_metrics(
        self, metrics_entry: MetricEntry
    ) -> Optional[FilteredEvent]:
        """
        Returns parsed fields dictionary from the metrics entry if the metrics entry is successfully parsed and filtered.
        """
        try:
            base_values = {
                self._replace_special_characters(
                    metrics_entry.metric_name
                ): metrics_entry.value
            }

            validated_fields = {}
            if metrics_entry.labels:
                if self.filter_config:
                    if not isinstance(self.filter_config, MetricFilterConfig):
                        raise ValueError(
                            "Cannot parse metrics without MetricFilterConfig"
                        )

                    parsed_metric = self.parser.parse_metric(
                        metrics_entry.labels,
                        version=self.filter_config.version,
                    )
                    fields = parsed_metric.parsed_fields

                    if not fields:
                        logger.info("No fields found in parsed metric")
                        return None
                else:
                    fields = metrics_entry.labels

                validated_fields = {
                    self._replace_special_characters(k): v for k, v in fields.items()
                }

            values = {**base_values, **validated_fields}

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
                "Metric parsing error",
                extra={"metric_content": metrics_entry, "error": str(e)},
            )
            return None
