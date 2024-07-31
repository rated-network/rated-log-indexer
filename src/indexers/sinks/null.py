from typing import Any, List, override
import structlog
from bytewax.outputs import DynamicSink, StatelessSinkPartition

logger = structlog.get_logger(__name__)


class _NullSinkPartition(StatelessSinkPartition[Any]):
    @override
    def write_batch(self, items: List[Any]) -> None:
        return None


class NullSink(DynamicSink[Any]):
    """Null sink that does nothing with the data. Useful for testing."""

    @override
    def build(self, step_id: str, worker_index: int, worker_count: int) -> _NullSinkPartition:
        return _NullSinkPartition()
