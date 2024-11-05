from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional, List, Any

import structlog
from bytewax.inputs import StatefulSourcePartition, FixedPartitionedSource
from pydantic import BaseModel, PositiveInt, StrictStr, StrictInt

from src.config.manager import ConfigurationManager
from src.indexers.offset_tracker.factory import get_offset_tracker
from src.utils.time_conversion import from_milliseconds, to_milliseconds

logger = structlog.get_logger(__name__)


class FetchInterval(int, Enum):
    MAX = 3_600  # 1 hour
    LOGS = 24
    METRICS = 60  # 1 minute

    def to_milliseconds(self):
        return self.value * 1000


class TimeRange(BaseModel):
    start_time: PositiveInt
    end_time: PositiveInt


class RatedPartition(StatefulSourcePartition[TimeRange, None]):
    BUFFER_MS = 60_000

    def __init__(self, slaos_key: StrictStr, config_index: StrictInt) -> None:
        self._next_awake = datetime.now(timezone.utc)

        self.config = ConfigurationManager().load_config().inputs

        self.offset_tracker, self.config_start_from = get_offset_tracker(
            slaos_key, config_index
        )
        self.config_type = self.config[config_index].type

        self.current_time = self._get_current_offset()
        self.timestamp = from_milliseconds(self.current_time)
        self.interval = (
            float(FetchInterval.LOGS)
            if self.config_type == "logs"
            else float(FetchInterval.METRICS)
        )
        logger.info(f"Indexing from {(self.current_time, self.timestamp)}")

    def _get_current_offset(self) -> PositiveInt:
        """
        Determines the starting offset for Cloudwatch log indexing.

        This method compares three potential starting points and selects the most recent (highest) one:
        1. The current offset from the offset tracker
        2. The start time specified in the configuration

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
        config_start_from_ms = self.config_start_from
        current_offset_ms = current_offset

        highest_offset = max(current_offset_ms, config_start_from_ms)
        if highest_offset > current_offset_ms:
            self.offset_tracker.update_offset(highest_offset)

        return highest_offset

    def _get_time_range(self) -> Optional[TimeRange]:
        """
        Fetches the next time range to index from integration.
        Uses MAX interval when backfilling, switches to smaller consistent intervals
        when close to real-time.
        """
        timestamp = datetime.now(timezone.utc)
        current_time_ms = to_milliseconds(timestamp)

        lag = current_time_ms - self.current_time

        if lag > FetchInterval.MAX.to_milliseconds():
            window_size = FetchInterval.MAX.to_milliseconds()
            logger.debug(
                f"Using MAX interval for backfill. Lag: {lag/1000:.2f} seconds"
            )
        else:
            window_size = int(self.interval * 1000)
            logger.debug(f"Using standard interval. Lag: {lag/1000:.2f} seconds")

        # Calculate head based on window size
        head = self.current_time + window_size

        # Don't go beyond current time
        head = min(head, current_time_ms)

        if head <= self.current_time:
            return None

        start_time = self.current_time
        self.current_time = head

        self.offset_tracker.update_offset(self.current_time)

        return TimeRange(start_time=start_time, end_time=head)

    def next_batch(self) -> List[TimeRange]:
        """
        Returns the next batch of time ranges to process.
        Uses minimal delay until caught up to real-time.
        """
        time_range = self._get_time_range()
        if not time_range:
            self._next_awake += timedelta(seconds=self.interval)
            return []

        # Calculate how far the end of our time range is from current time
        current_time_ms = to_milliseconds(datetime.now(timezone.utc))
        lag = current_time_ms - time_range.end_time

        # If we're behind by more than our standard interval, use minimal delay
        if lag > (self.interval * 1000 + self.BUFFER_MS):
            self._next_awake += timedelta(seconds=2.0)  # Minimal delay for catching up
            logger.debug(f"Using minimal delay. Current lag: {lag / 1000:.2f} seconds")
        else:
            # We're caught up - use normal interval delay
            self._next_awake += timedelta(seconds=self.interval)
            logger.debug(f"Using standard delay. Current lag: {lag / 1000:.2f} seconds")

        return [time_range]

    def next_awake(self):
        return self._next_awake

    def snapshot(self):
        return None


class RatedSource(FixedPartitionedSource[TimeRange, None]):
    """
    Yields time ranges. Continuously polls the source for the new head,
    emits a safe range to fetch
    """

    def __init__(self, slaos_key: str, config_index: int):
        self.slaos_key = slaos_key
        self.config_index = config_index

    def list_parts(self):
        return ["single-part"]

    def build_part(self, step_id: StrictStr, for_key: StrictStr, resume_state: Any):
        assert for_key == "single-part"
        return RatedPartition(self.slaos_key, self.config_index)
