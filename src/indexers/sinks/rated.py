import asyncio
from typing import Any, List
import time
from collections import deque
import structlog
import httpx
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from dataclasses import dataclass

from src.config.models.output import RatedOutputConfig
from src.indexers.filters.types import FilteredEvent

logger = structlog.get_logger(__name__)


@dataclass
class SlaOsApiBody:
    customer_id: str
    timestamp: str
    key: str
    values: dict


class _HTTPSinkPartition(StatelessSinkPartition):
    """
    Stateless partition responsible for batching and sending events to an HTTP endpoint.
    It manages a batch of events, flushing them when the batch size is reached or when a timeout occurs.
    """

    def __init__(
        self,
        config: RatedOutputConfig,
        worker_index: int,
    ) -> None:
        """
        Initialize the HTTP sink partition with the given configuration and worker index.

        Args:
            config (RatedOutputConfig): Configuration object containing endpoint details.
            worker_index (int): The index of the worker running this partition.
        """
        super().__init__()
        self.worker_index = worker_index
        self.config = config
        self.client = httpx.AsyncClient()
        self.max_concurrent_requests = 5
        self.batch_size: int = 50
        self.batch_timeout_seconds: int = 10
        self.batch: Any = deque()
        self.last_flush_time: float = time.time()
        self.flush_in_progress: bool = False
        logger.debug(
            f"Worker {self.worker_index} initialized",
            http_endpoint=self.config.ingestion_url,
        )

    async def _flush_if_needed(self):
        """
        Check if the current batch needs to be flushed based on size or timeout.
        If a flush is needed and not already in progress, it triggers the flush operation.
        """
        if self.should_flush() and not self.flush_in_progress:
            self.flush_in_progress = True
            await self.flush_batch()
            self.flush_in_progress = False

    def _compose_body(self, items: List[FilteredEvent]) -> List[dict]:
        """
        Compose the HTTP request body from a list of FilteredEvent items.

        Args:
            items (List[FilteredEvent]): List of events to be sent in the batch.

        Returns:
            List[dict]: The HTTP request body in dictionary format.
        """
        return [
            SlaOsApiBody(
                customer_id=item.customer_id,
                timestamp=item.event_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
                key=self.config.ingestion_key,
                values=item.values,
            ).__dict__
            for item in items
        ]

    def _compose_headers(self) -> dict:
        """
        Compose the HTTP request headers.

        Returns:
            dict: The HTTP headers including content type.
        """
        return {
            "Content-Type": "application/json",
        }

    def _compose_url(self) -> str:
        """
        Compose the target URL for the HTTP request.

        Returns:
            str: The complete URL to send the request to.
        """
        return f"{self.config.ingestion_url}/{self.config.ingestion_id}/{self.config.ingestion_key}"

    async def send_batch(self, items: List[FilteredEvent]) -> None:
        """
        Send a batch of events to the HTTP endpoint.

        Args:
            items (List[FilteredEvent]): List of events to be sent.

        Raises:
            httpx.HTTPError: If the HTTP request fails.
        """
        try:
            body = self._compose_body(items)
            headers = self._compose_headers()
            url = self._compose_url()
            logger.info(f"Sending batch of {len(items)} items to {url}")
            response = await self.client.post(url, json=body, headers=headers)
            response.raise_for_status()
            logger.debug(
                f"Worker {self.worker_index} successfully sent batch to HTTP endpoint",
                batch_size=len(items),
            )
        except httpx.HTTPError as e:
            logger.error(f"Worker {self.worker_index} HTTP error: {e}", items=items)
            raise
        except Exception as e:
            logger.error(f"Worker {self.worker_index} error: {e}", items=items)

    async def flush_batch(self) -> None:
        """
        Flush the current batch of events. Sends them to the HTTP endpoint and clears the batch.
        """
        if self.batch:
            items = list(self.batch)
            self.batch.clear()
            await self.send_batch(items)
            self.last_flush_time = time.time()
            logger.debug(f"Flushed batch of {len(items)} items")
        else:
            logger.debug("No items to flush")

    def should_flush(self) -> bool:
        """
        Determine if the batch should be flushed based on its size or the time since the last flush.

        Returns:
            bool: True if the batch should be flushed, otherwise False.
        """
        return len(self.batch) >= self.batch_size or (
            time.time() - self.last_flush_time >= self.batch_timeout_seconds
            and self.batch
        )

    def write(self, item: Any) -> None:
        """
        Add an item to the current batch and trigger a flush if needed.

        Args:
            item (Any): The event to be added to the batch.
        """
        self.batch.append(item)
        logger.debug(f"Added item to batch. Current batch size: {len(self.batch)}")
        # Schedule flush if needed
        asyncio.run(self._flush_if_needed())

    def write_batch(self, items: List[Any]) -> None:
        """
        Add a batch of items to the current batch and trigger a flush if needed.

        Args:
            items (List[Any]): List of events to be added to the batch.
        """
        for item in items:
            self.batch.append(item)
            logger.debug(f"Added item to batch. Current batch size: {len(self.batch)}")
        # Flush any remaining items after batch write
        asyncio.run(self._flush_if_needed())

    def close(self):
        """
        Close the HTTP client and ensure any remaining items are flushed.
        """
        if self.batch:
            logger.info("Flushing remaining items in close")
            asyncio.run(self.flush_batch())
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
