from typing import Any, Dict, Iterator

import structlog
from rated_exporter_sdk.providers.prometheus.client import PrometheusClient  # type: ignore
from rated_exporter_sdk.providers.prometheus.types import PrometheusQueryOptions  # type: ignore

from src.config.models.inputs.prometheus import PrometheusConfig
from src.utils.time_conversion import from_milliseconds

logger = structlog.get_logger(__name__)


class PrometheusClientWrapper:
    """Wrapper for the Prometheus SDK client to match the interface expected by the indexer."""

    def __init__(self, config: PrometheusConfig):
        self.config = config
        self.client = PrometheusClient(
            base_url=config.base_url,
            auth=config.auth,
            timeout=config.timeout or 15.0,
            max_retries=config.max_retries,
            retry_backoff_factor=config.retry_backoff_factor,
            pool_connections=config.pool_connections,
            pool_maxsize=config.pool_maxsize,
            max_parallel_queries=config.max_parallel_queries,
        )

    def query_metrics(self, start_time: int, end_time: int) -> Iterator[Dict[str, Any]]:
        """Query metrics from Prometheus according to the configuration."""
        start_datetime = from_milliseconds(start_time)
        end_datetime = from_milliseconds(end_time)

        for query_config in self.config.queries:
            try:
                options = PrometheusQueryOptions(
                    start_time=start_datetime,
                    end_time=end_datetime,
                    step=query_config.step,
                    timeout=self.config.timeout,
                )

                result = self.client.query_range(query_config.query, options)

                for metric in result.metrics:
                    # Get the organization ID from the labels using the configured identifier
                    org_id = metric.identifier.labels.get(
                        query_config.organization_identifier
                    )
                    if not org_id:
                        logger.warning(
                            "Organization identifier not found in metric labels",
                            metric_name=metric.identifier.name,
                            organization_identifier=query_config.organization_identifier,
                        )
                        continue

                    for sample in metric.samples:
                        yield {
                            "organization_id": org_id,
                            "timestamp": sample.timestamp,
                            "value": sample.value,
                            "label": query_config.slaos_metric_name,
                            "labels": metric.identifier.labels,
                        }

            except Exception as e:
                logger.error(
                    "Failed to execute Prometheus query",
                    query=query_config.query,
                    error=str(e),
                    exc_info=True,
                )
                continue

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()
