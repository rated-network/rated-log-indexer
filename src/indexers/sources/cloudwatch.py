from datetime import datetime, timezone, timedelta
from typing import Optional, List, Any

from bytewax.inputs import StatefulSourcePartition, FixedPartitionedSource
from pydantic import BaseModel, PositiveInt, StrictStr

from clients.cloudwatch import cloudwatch_client
from utils.logger import logger
from utils.time_conversion import from_milliseconds, to_milliseconds

DAY_INTERVAL = 86_400_000


class TimeRange(BaseModel):
    start_time: PositiveInt
    end_time: PositiveInt


class CloudwatchPartition(StatefulSourcePartition[TimeRange, None]):
    def __init__(self, start_from: PositiveInt):
        self.client = cloudwatch_client()
        self._next_awake = datetime.now(timezone.utc)
        self.current_time = start_from
        self.timestamp = from_milliseconds(self.current_time)
        logger.info(
            f"Starting from Cloudwatch indexing from {(self.current_time, self.timestamp)}"
        )

    def cloudwatch_time_range(self) -> Optional[TimeRange]:
        """
        Fetches the next time range to index from Cloudwatch, with a max of a day's worth of logs.
        """

        timestamp = datetime.now(timezone.utc)
        head = to_milliseconds(timestamp)
        logger.info(
            f"Latest timestamp is {timestamp}. Current timestamp for indexing is {self.current_time}"
        )

        if head - self.current_time > DAY_INTERVAL:
            head = self.current_time + DAY_INTERVAL
        if head <= self.current_time:
            return None

        start_time = self.current_time + 1
        self.current_time = head

        logger.info(f"Fetching Cloudwatch logs from {start_time} to {head}")

        return TimeRange(start_time=start_time, end_time=head)

    def next_batch(self) -> List[TimeRange]:
        time_range = self.cloudwatch_time_range()
        if not time_range:
            self._next_awake += timedelta(seconds=24.0)
            return []
        if time_range.start_time - time_range.end_time == DAY_INTERVAL:
            self._next_awake += timedelta(
                seconds=2.0
            )  # Processing historical logs from Cloudwatch, so we can fetch more frequently
        else:
            self._next_awake += timedelta(seconds=24.0)
        return [time_range]

    def next_awake(self):
        return self._next_awake

    def snapshot(self):
        return None


class CloudwatchSource(FixedPartitionedSource[TimeRange, None]):
    """
    Yields time ranges. Continuously polls the source for the new head,
    emits a safe range to fetch
    """

    def __init__(self, start_from: PositiveInt):
        self.start_from = start_from

    def list_parts(self):
        return ["single-part"]

    def build_part(self, step_id: StrictStr, for_key: StrictStr, resume_state: Any):
        assert for_key == "single-part"
        return CloudwatchPartition(start_from=self.start_from)
