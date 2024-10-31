import structlog
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Any, Union, Optional

from src.utils.time_conversion import from_milliseconds

logger = structlog.get_logger(__name__)


def generate_idempotency_key(
    event_timestamp: datetime, organization_id: str, values: dict
) -> str:
    """
    Generate an idempotency key based on event timestamp, customer ID, and values.
    """
    timestamp_str = event_timestamp.isoformat()
    sorted_values = json.dumps(values, sort_keys=True)
    components = f"{timestamp_str}|{organization_id}|{sorted_values}"
    return hashlib.sha256(components.encode()).hexdigest()


@dataclass
class LogEntry:
    log_id: str
    content: Union[str, dict]
    is_json: bool
    metadata: dict
    event_timestamp: datetime

    @classmethod
    def from_cloudwatch_log(cls, log: Dict[str, Any]) -> "LogEntry":
        content: Union[str, dict]
        try:
            content = json.loads(log["message"])
            is_json = True
        except json.JSONDecodeError:
            content = log["message"]
            is_json = False

        event_timestamp = datetime.fromtimestamp(
            log["timestamp"] / 1000, tz=timezone.utc
        )

        return cls(
            log_id=log["eventId"],
            content=content,
            is_json=is_json,
            metadata={
                "log_stream_name": log.get("logStreamName", ""),
            },
            event_timestamp=event_timestamp,
        )

    @classmethod
    def from_datadog_log(cls, log: Dict[str, Any]) -> "LogEntry":
        content: Union[str, dict]
        log_attributes = log.get("attributes", {})

        if not log_attributes:
            logger.error("Datadog log attributes are missing.", exc_info=True)
            raise

        content = log_attributes.get("attributes")

        if content and isinstance(content, dict):
            is_json = True
        else:
            is_json = False

        try:
            # The timestamp object returned from Datadog is in timezone.localtime, but they index in UTC
            event_timestamp = log_attributes.get("timestamp").replace(
                tzinfo=timezone.utc
            )
        except AttributeError as e:
            msg = f"Failed to parse Datadog log timestamp. Received: {log_attributes.get('timestamp')}"
            logger.error(msg, exc_info=True)
            raise ValueError(msg) from e

        return cls(
            log_id=log["id"],
            content=content,
            is_json=is_json,
            metadata={
                "service": log_attributes.get("service", ""),
                "status": log_attributes.get("status", ""),
                "tags": log_attributes.get("tags", []),
            },
            event_timestamp=event_timestamp,
        )


@dataclass
class MetricEntry:
    metric_name: str
    value: float
    organization_id: str
    event_timestamp: datetime
    labels: Optional[Dict[str, Any]] = None

    @classmethod
    def from_cloudwatch_metric(cls, metric: Dict[str, Any]) -> "MetricEntry":
        return cls(
            metric_name=metric["label"],
            value=metric["value"],
            organization_id=metric["organization_id"],
            event_timestamp=metric["timestamp"],
        )

    @classmethod
    def from_datadog_metric(cls, metric: Dict[str, Any]) -> "MetricEntry":
        return cls(
            metric_name=metric["metric_name"],
            value=metric["value"],
            organization_id=metric["organization_id"],
            event_timestamp=from_milliseconds(metric["timestamp"]),
        )


@dataclass
class FilteredEvent:
    integration_prefix: str
    idempotency_key: str
    event_timestamp: datetime
    organization_id: str
    values: dict
