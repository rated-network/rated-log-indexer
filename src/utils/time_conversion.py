from datetime import datetime, timezone

from pydantic import PositiveInt


def to_milliseconds(dt: datetime) -> PositiveInt:
    # Convert the datetime object to a timestamp (seconds since epoch)
    timestamp = dt.timestamp()
    milliseconds = int(timestamp * 1000)
    return milliseconds


def from_milliseconds(ms: PositiveInt) -> datetime:
    # Convert milliseconds to datetime object
    seconds = ms / 1000.0
    dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
    return dt
