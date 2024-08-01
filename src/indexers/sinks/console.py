from typing import Any, List
import structlog
from bytewax.outputs import DynamicSink, StatelessSinkPartition

logger = structlog.get_logger(__name__)


class _ConsoleSinkPartition(StatelessSinkPartition):
    def __init__(self, worker_index: int) -> None:
        super().__init__()
        self.worker_index = worker_index
        logger.info(f"Worker {self.worker_index} initialized for console output")

    def write_batch(self, items: List[Any]) -> None:
        for item in items:
            print(f"Worker {self.worker_index}: {item}")

    def close(self):
        logger.info(f"Worker {self.worker_index} Console sink closed")


class ConsoleSink(DynamicSink):
    def build(self, step_id: str, worker_index: int, worker_count: int):
        return _ConsoleSinkPartition(worker_index)


def build_console_sink() -> ConsoleSink:
    return ConsoleSink()
