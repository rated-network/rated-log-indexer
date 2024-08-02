from abc import ABC, abstractmethod
from typing import Union
from datetime import datetime, timedelta

from pydantic import StrictInt

from src.config.manager import ConfigurationManager
from src.config.models.offset import OffsetYamlConfig


class OffsetTracker(ABC):
    def __init__(self):
        self.config: OffsetYamlConfig = ConfigurationManager.load_config().offset
        self.start_from: Union[datetime, StrictInt] = self.config.start_from

    @abstractmethod
    def get_current_offset(self) -> Union[int, datetime]:
        """Retrieve the current offset."""
        pass

    @abstractmethod
    def update_offset(self, offset: Union[int, datetime]) -> None:
        """Update the current offset."""
        pass

    def get_time_range(self, max_window: Union[int, timedelta]) -> tuple:
        current_offset = self.get_current_offset()
        if current_offset is None:
            current_offset = self.start_from

        if isinstance(current_offset, int):
            end_point: Union[int, datetime] = current_offset + (
                max_window
                if isinstance(max_window, int)
                else int(max_window.total_seconds())
            )
        else:  # datetime
            end_point = current_offset + (
                max_window
                if isinstance(max_window, timedelta)
                else timedelta(seconds=max_window)
            )

        return current_offset, end_point
