import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Any, Union


@dataclass
class LogEntry:
    log_id: str
    content: Union[str, dict]
    is_json: bool
    metadata: dict
    event_timestamp: datetime

    @classmethod
    def from_cloudwatch_log(cls, log: Dict[str, Any]) -> 'LogEntry':

        try:
            content: dict = json.loads(log['message'])
            is_json = True
        except json.JSONDecodeError:
            content: str = log['message']
            is_json = False

        event_timestamp = datetime.fromtimestamp(log['timestamp'] / 1000, tz=timezone.utc)

        return cls(
            log_id=log.get('eventId', ''),
            content=content,
            is_json=is_json,
            metadata={
                'log_stream_name': log.get('logStreamName', ''),
            },
            event_timestamp=event_timestamp
        )

