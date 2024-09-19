from datetime import datetime, timezone, timedelta
from typing import Optional, List, Any

import structlog
from bytewax.inputs import StatefulSourcePartition, FixedPartitionedSource
from pydantic import BaseModel, PositiveInt, StrictStr

from src.config.manager import ConfigurationManager
from src.indexers.offset_tracker.factory import get_offset_tracker
from src.utils.time_conversion import from_milliseconds, to_milliseconds

logger = structlog.get_logger(__name__)

DAY_INTERVAL = 86_400_000


class TimeRange(BaseModel):
    start_time: PositiveInt
    end_time: PositiveInt


class RatedPartition(StatefulSourcePartition[TimeRange, None]):
    def __init__(self, integration_prefix: StrictStr) -> None:
        self._next_awake = datetime.now(timezone.utc)

        self.config = ConfigurationManager().load_config().inputs

        self.offset_tracker, self.config_start_from = get_offset_tracker(
            integration_prefix
        )

        self.current_time = self._get_current_offset()
        self.timestamp = from_milliseconds(self.current_time)
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
        Fetches the next time range to index from integration, with a max of a day's worth of logs.
        """

        timestamp = datetime.now(timezone.utc)
        head = to_milliseconds(timestamp)

        if head - self.current_time > DAY_INTERVAL:
            head = self.current_time + DAY_INTERVAL
        if head <= self.current_time:
            return None

        start_time = self.current_time + 1
        self.current_time = head

        self.offset_tracker.update_offset(self.current_time)

        return TimeRange(start_time=start_time, end_time=head)

    def next_batch(self) -> List[TimeRange]:
        time_range = self._get_time_range()
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


class RatedSource(FixedPartitionedSource[TimeRange, None]):
    """
    Yields time ranges. Continuously polls the source for the new head,
    emits a safe range to fetch
    """

    def __init__(self, integration_prefix: str):
        self.integration_prefix = integration_prefix

    def list_parts(self):
        return ["single-part"]

    def build_part(self, step_id: StrictStr, for_key: StrictStr, resume_state: Any):
        assert for_key == "single-part"
        return RatedPartition(self.integration_prefix)
