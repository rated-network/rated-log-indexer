import json
from typing import Any, List, Dict, Iterator, Tuple, Optional
import time
from collections import deque

import stamina
import structlog
import httpx
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from dataclasses import dataclass

from pydantic import StrictInt, StrictBool, StrictFloat, StrictStr


from src.config.models.output import RatedOutputConfig
from src.indexers.filters.types import FilteredEvent

logger = structlog.get_logger(__name__)


@dataclass
class SlaOsApiBody:
    organization_id: str
    timestamp: str
    key: str
    idempotency_key: str
    values: dict

    @classmethod
    def parse_and_prefix_values(
        cls, raw_values: Any, integration_prefix: Optional[StrictStr]
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
            return {f"{integration_prefix.strip()}_{k}": v for k, v in values.items()}
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
            organization_id=event.organization_id,
            timestamp=event.event_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
            key=key,
            idempotency_key=event.idempotency_key,
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
        self.client = httpx.Client()
        self.max_concurrent_requests = 5
        self.batch_size: StrictInt = 50
        self.batch_timeout_seconds: StrictInt = 10
        self.batch: Any = deque()
        self.last_flush_time: StrictFloat = time.time()
        self.flush_in_progress: StrictBool = False
        logger.debug(
            f"Worker {self.worker_index} initialized",
            http_endpoint=self.config.ingestion_url,
        )

    def _compose_body(self, items: List[FilteredEvent]) -> List[dict]:
        """
        Compose the HTTP request body from a list of FilteredEvent items.

        Args:
            items (List[FilteredEvent]): List of events to be sent in the batch.

        Returns:
            List[dict]: The HTTP request body in dictionary format.
        """
        body = []

        for item in items:
            event_data = {
                "organization_id": item.organization_id,
                "timestamp": item.event_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "key": (
                    item.integration_prefix
                    if item.integration_prefix
                    else "a_valid_source"
                ),
                "idempotency_key": item.idempotency_key,
            }
            prefixed_values: dict = SlaOsApiBody.parse_and_prefix_values(
                item.values, None
            )
            reserved_keys = [
                f"{item.integration_prefix}_organization_id",
                f"{item.integration_prefix}_timestamp",
                f"{item.integration_prefix}_key",
                f"{item.integration_prefix}_idempotency_key",
                "key",
                "organization_id",
                "timestamp",
                "idempotency_key",
            ]
            event_data["values"] = {  # type: ignore
                k: v for k, v in prefixed_values.items() if k not in reserved_keys
            }

            body.append(event_data)

        return body

    def _compose_headers(self) -> dict:
        """
        Compose the HTTP request headers.

        Returns:
            dict: The HTTP headers including content type.
        """
        return {
            "Content-Type": "application/json",
        }

    def _compose_url(self) -> Tuple[str, str]:
        """
        Compose the target URL for the HTTP request and a redacted version for logging.

        Returns:
            Tuple[str, str]: A tuple containing (full_url, redacted_url)
        """
        ingestion_id = self.config.ingestion_id
        ingestion_key = self.config.ingestion_key

        full_url = f"{self.config.ingestion_url}/{ingestion_id}/{ingestion_key}"

        redacted_id = ingestion_id[:5] + "*" * 3
        redacted_key = ingestion_key[:5] + "*" * 3
        redacted_url = f"{self.config.ingestion_url}/{redacted_id}/{redacted_key}"

        return full_url, redacted_url

    def write(self, item: Dict) -> None:
        """
        Process a single item.
        """
        self.process_items(iter([item]))

    def write_batch(self, items: List[Dict]) -> None:
        """
        Process a batch of items.
        """
        self.process_items(iter(items))

    def process_items(self, items_iterator: Iterator[Dict]) -> None:
        """
        Process items using an iterator, flushing when necessary.
        """
        for item in items_iterator:
            self.batch.append(item)
            if self.should_flush():
                self.flush_batch()

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

    def flush_batch(self) -> None:
        """
        Flush the current batch of events.
        """
        if self.batch:
            self.send_batch(self.batch)
            self.batch = []
            self.last_flush_time = time.time()

    @stamina.retry(on=Exception, attempts=5)
    def send_batch(self, items: List[FilteredEvent]) -> None:
        """
        Send a batch of events to the HTTP endpoint.
        """
        integration_prefixes = {item.integration_prefix for item in items}
        try:
            body = self._compose_body(items)
            headers = self._compose_headers()
            url, redacted_url = self._compose_url()
            response = self.client.post(url, json=body, headers=headers)
            response.raise_for_status()
            logger.info(
                "Successfully sent batch to slaOS",
                batch_size=len(items),
                redacted_url=redacted_url,
                worker_index=self.worker_index,
                integration_prefix=integration_prefixes,
            )

        except httpx.HTTPError as e:
            print(response.text)
            logger.error(
                f"Worker {self.worker_index} HTTP error sending batch: {e}",
                integration_prefix=integration_prefixes,
                batch_size=len(items),
            )
            raise
        except Exception as e:
            logger.error(
                f"Worker {self.worker_index} error sending batch: {e}",
                integration_prefix=integration_prefixes,
                batch_size=len(items),
            )
            raise

    def close(self):
        """
        Flush any remaining items and close the HTTP client.
        """
        self.flush_batch()
        self.client.close()
        logger.info(
            f"Worker {self.worker_index} HTTP sink closed",
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
