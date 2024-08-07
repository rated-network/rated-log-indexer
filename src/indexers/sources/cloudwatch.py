from datetime import datetime, timezone, timedelta
from typing import Optional, List, Any

from bytewax.inputs import StatefulSourcePartition, FixedPartitionedSource
from pydantic import BaseModel, PositiveInt, StrictStr

from src.clients.cloudwatch import get_cloudwatch_client
from src.indexers.offset_tracker.factory import get_offset_tracker
from src.utils.logger import logger
from src.utils.time_conversion import from_milliseconds, to_milliseconds

DAY_INTERVAL = 86_400_000


class TimeRange(BaseModel):
    start_time: PositiveInt
    end_time: PositiveInt


class CloudwatchPartition(StatefulSourcePartition[TimeRange, None]):
    def __init__(self, start_from: Optional[PositiveInt] = None) -> None:
        self.client = get_cloudwatch_client()
        self._next_awake = datetime.now(timezone.utc)

        self.offset_tracker, self.config_start_from = get_offset_tracker()

        self.current_time = self._get_current_offset(start_from)
        self.timestamp = from_milliseconds(self.current_time)
        logger.info(
            f"Starting Cloudwatch indexing from {(self.current_time, self.timestamp)}"
        )

    def _get_current_offset(self, start_from: Optional[PositiveInt]) -> PositiveInt:
        """
        Determines the starting offset for Cloudwatch log indexing.

        This method compares three potential starting points and selects the most recent (highest) one:
        1. The current offset from the offset tracker
        2. The start time specified in the configuration
        3. An optional start time passed as an argument

        The method ensures we always start indexing from the latest acceptable point,
        preventing accidental processing of data from before the intended start time.

        Parameters:
        start_from (Optional[PositiveInt]): An optional starting timestamp in milliseconds.
                                            If provided, it will be considered in determining
                                            the starting offset.

        Returns:
        PositiveInt: The selected starting offset in milliseconds.

        """
        current_offset = self.offset_tracker.get_current_offset()
        config_start_from_ms = (
            to_milliseconds(self.config_start_from)
            if isinstance(self.config_start_from, datetime)
            else self.config_start_from
        )
        current_offset_ms = (
            to_milliseconds(current_offset)
            if isinstance(current_offset, datetime)
            else current_offset
        )
        start_from_ms = start_from if start_from is not None else 0

        highest_offset = max(current_offset_ms, config_start_from_ms, start_from_ms)
        if highest_offset > current_offset_ms:
            self.offset_tracker.update_offset(highest_offset)

        return highest_offset

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

        self.offset_tracker.update_offset(self.current_time)

        logger.info(f"Fetching Cloudwatch logs from {start_time} to {head}")

        return TimeRange(start_time=start_time, end_time=head)

    def next_batch(self) -> List[TimeRange]:
        time_range = self.cloudwatch_time_range()
        if not time_range:
            self._next_awake += timedelta(seconds=24.0)
            return []
        if time_range.start_time - time_range.end_time == DAY_INTERVAL:
            self._next_awake += timedelta(seconds=2.0)
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

    def __init__(self, start_from: Optional[PositiveInt] = None):
        self.start_from = start_from

    def list_parts(self):
        return ["single-part"]

    def build_part(self, step_id: StrictStr, for_key: StrictStr, resume_state: Any):
        assert for_key == "single-part"
        return CloudwatchPartition(start_from=self.start_from)
