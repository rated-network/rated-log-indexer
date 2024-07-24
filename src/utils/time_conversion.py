from datetime import datetime

from pydantic import PositiveInt


def to_milliseconds(dt: datetime) -> PositiveInt:
    # Convert the datetime object to a timestamp (seconds since epoch)
    timestamp = dt.timestamp()
    milliseconds = int(timestamp * 1000)
    return milliseconds
