from abc import ABC, abstractmethod
from datetime import timedelta

from pydantic import StrictStr

from src.config.models.offset import OffsetYamlConfig


class OffsetTracker(ABC):
    def __init__(self, config: OffsetYamlConfig, slaos_key: StrictStr):
        self.slaos_key = slaos_key
        self.config = config

    @abstractmethod
    def get_current_offset(self) -> int:
        """Retrieve the current offset."""
        pass

    @abstractmethod
    def update_offset(self, offset: int) -> None:
        """Update the current offset."""
        pass

    def get_time_range(self, max_window: int) -> tuple[int, int]:
        """Get the time range for the current offset."""
        current_offset = self.get_current_offset()
        if current_offset is None:
            current_offset = self.config.start_from

        if isinstance(max_window, timedelta):
            max_window = int(max_window.total_seconds())

        end_point = current_offset + max_window

        return current_offset, end_point
