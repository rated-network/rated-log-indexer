import asyncio
import json
from typing import Any, List, Dict
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

    @classmethod
    def parse_and_prefix_values(
        cls, raw_values: Any, integration_prefix: str
    ) -> Dict[str, Any]:
        """
        Parse the values if they're a string, and add the integration prefix to the keys.

        Args:
            raw_values (Any): The values to parse and prefix.
            integration_prefix (str): The prefix to add to each key.

        Returns:
            Dict[str, Any]: A dictionary with prefixed keys.
        """
        if isinstance(raw_values, str):
            try:
                values = json.loads(raw_values)
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON string for values: {raw_values}")
                return {}
        elif isinstance(raw_values, dict):
            values = raw_values
        else:
            logger.error(f"Unexpected type for values: {type(raw_values)}")
            return {}

        if integration_prefix and integration_prefix.strip():
            return {f"{integration_prefix.strip()}.{k}": v for k, v in values.items()}
        return values

    @classmethod
    def from_filtered_event(
        cls, event: FilteredEvent, key: str, integration_prefix: str
    ) -> "SlaOsApiBody":
        """
        Create a SlaOsApiBody instance from a FilteredEvent.

        Args:
            event (FilteredEvent): The filtered event to convert.
            key (str): The key to use for the SlaOsApiBody.
            integration_prefix (str): The integration prefix to apply to values.

        Returns:
            SlaOsApiBody: A new instance of SlaOsApiBody.
        """
        return cls(
            customer_id=event.customer_id,
            timestamp=event.event_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
            key=key,
            values=cls.parse_and_prefix_values(event.values, integration_prefix),
        )


class _HTTPSinkPartition(StatelessSinkPartition):
    """
    Stateless partition responsible for batching and sending events to an HTTP endpoint.
    It manages a batch of events, flushing them when the batch size is reached or when a timeout occurs.
    """

    def __init__(
        self,
        config: RatedOutputConfig,
        integration_prefix: str,
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
        self.integration_prefix = integration_prefix
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
            integration_prefix=self.integration_prefix,
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
            SlaOsApiBody.from_filtered_event(
                event=item,
                key=self.config.ingestion_key,
                integration_prefix=self.integration_prefix,
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
                integration_prefix=self.integration_prefix,
            )
        except httpx.HTTPError as e:
            logger.error(
                f"Worker {self.worker_index} HTTP error: {e}",
                items=items,
                integration_prefix=self.integration_prefix,
            )
            raise
        except Exception as e:
            logger.error(
                f"Worker {self.worker_index} error: {e}",
                items=items,
                integration_prefix=self.integration_prefix,
            )

    async def flush_batch(self) -> None:
        """
        Flush the current batch of events. Sends them to the HTTP endpoint and clears the batch.
        """
        if self.batch:
            items = list(self.batch)
            self.batch.clear()
            await self.send_batch(items)
            self.last_flush_time = time.time()
            logger.debug(
                f"Flushed batch of {len(items)} items",
                integration_prefix=self.integration_prefix,
            )
        else:
            logger.debug(
                "No items to flush", integration_prefix=self.integration_prefix
            )

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
        logger.debug(
            f"Added item to batch. Current batch size: {len(self.batch)}",
            integration_prefix=self.integration_prefix,
        )
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
            logger.debug(
                f"Added item to batch. Current batch size: {len(self.batch)}",
                integration_prefix=self.integration_prefix,
            )
        # Flush any remaining items after batch write
        asyncio.run(self._flush_if_needed())

    def close(self):
        """
        Close the HTTP client and ensure any remaining items are flushed.
        """
        if self.batch:
            logger.info(
                "Flushing remaining items in close",
                integration_prefix=self.integration_prefix,
            )
            asyncio.run(self.flush_batch())
        asyncio.run(self.client.aclose())
        logger.info(
            f"Worker {self.worker_index} HTTP sink closed",
            integration_prefix=self.integration_prefix,
        )


class HTTPSink(DynamicSink):
    def __init__(self, config: RatedOutputConfig, integration_prefix: str) -> None:
        super().__init__()
        self.config = config
        self.integration_prefix = integration_prefix

    def build(self, step_id: str, worker_index: int, worker_count: int):
        return _HTTPSinkPartition(self.config, self.integration_prefix, worker_index)


def build_http_sink(config: RatedOutputConfig, integration_prefix: str) -> HTTPSink:
    return HTTPSink(config=config, integration_prefix=integration_prefix)
