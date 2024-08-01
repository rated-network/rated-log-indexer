import asyncio
from typing import Any, List
import structlog
import httpx
from bytewax.outputs import DynamicSink, StatelessSinkPartition

logger = structlog.get_logger(__name__)


class HTTPConfig:
    def __init__(self, endpoint: str, max_concurrent_requests: int = 10):
        self.endpoint = endpoint
        self.max_concurrent_requests = max_concurrent_requests


class _HTTPSinkPartition(StatelessSinkPartition):
    def __init__(
        self,
        config: HTTPConfig,
        worker_index: int,
    ) -> None:
        super().__init__()
        self.worker_index = worker_index
        self.endpoint = config.endpoint
        self.max_concurrent_requests = config.max_concurrent_requests
        self.client = httpx.AsyncClient()
        logger.info(
            f"Worker {self.worker_index} initialized", http_endpoint=self.endpoint
        )

    async def send_item(self, item: Any) -> None:
        try:
            response = await self.client.post(self.endpoint, json=item)
            response.raise_for_status()
        except httpx.HTTPError:
            logger.error(f"Worker {self.worker_index} HTTP error", item=item)
            raise

    async def send_batch(self, items: List[Any]) -> None:
        semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        async with semaphore:
            tasks = [self.send_item(item) for item in items]
            await asyncio.gather(*tasks)

    def write_batch(self, items: List[Any]) -> None:
        try:
            asyncio.run(self.send_batch(items))
            logger.info(
                f"Worker {self.worker_index} successfully sent batch to HTTP endpoint"
            )
        except Exception:
            logger.error(
                f"Worker {self.worker_index} Failed to send batch to HTTP endpoint",
                exc_info=True,
            )
            raise Exception(
                "Critical error: Failed to send batch to HTTP endpoint. Aborting stream"
            )

    def close(self):
        asyncio.run(self.client.aclose())
        logger.info(f"Worker {self.worker_index} HTTP sink closed")


class HTTPSink(DynamicSink):
    def __init__(self, config: HTTPConfig) -> None:
        super().__init__()
        self.config = config

    def build(self, step_id: str, worker_index: int, worker_count: int):
        return _HTTPSinkPartition(self.config, worker_index)


def build_http_sink(endpoint: str, max_concurrent_requests: int = 10) -> HTTPSink:
    config = HTTPConfig(endpoint, max_concurrent_requests)
    return HTTPSink(config=config)
