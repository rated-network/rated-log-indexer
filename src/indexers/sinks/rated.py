import asyncio
from typing import Any, List
import structlog
import httpx
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from dataclasses import dataclass

from src.config.models.output import RatedOutputConfig
from src.indexers.filters.manager import FilteredEvent

logger = structlog.get_logger(__name__)


@dataclass
class SlaOsApiBody:
    customer_id: str
    timestamp: str
    key: str
    values: dict


class _HTTPSinkPartition(StatelessSinkPartition):
    def __init__(
        self,
        config: RatedOutputConfig,
        worker_index: int,
    ) -> None:
        super().__init__()
        self.worker_index = worker_index
        self.config = config
        self.client = httpx.AsyncClient()
        self.max_concurrent_requests = 5
        logger.info(
            f"Worker {self.worker_index} initialized",
            http_endpoint=self.config.ingestion_url,
        )

    def _compose_body(self, items: FilteredEvent) -> dict:
        return SlaOsApiBody(
            customer_id=items.customer_id,
            timestamp=items.event_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
            key=self.config.ingestion_key,
            values=items.values,
        ).__dict__

    def _compose_headers(self) -> dict:
        return {
            "Content-Type": "application/json",
        }

    def _compose_url(self) -> str:
        return f"{self.config.ingestion_url}/{self.config.ingestion_id}/{self.config.ingestion_key}"

    async def send_item(self, item: FilteredEvent) -> None:

        try:
            body = self._compose_body(item)
            headers = self._compose_headers()
            url = self._compose_url()
            response = await self.client.post(url, json=body, headers=headers)
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
    def __init__(self, config: RatedOutputConfig) -> None:
        super().__init__()
        self.config = config

    def build(self, step_id: str, worker_index: int, worker_count: int):
        return _HTTPSinkPartition(self.config, worker_index)


def build_http_sink(config: RatedOutputConfig) -> HTTPSink:
    return HTTPSink(config=config)
