import sys
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
            base_url=str(config.base_url),
            auth=config.auth,
            timeout=config.timeout,
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
                    # TODO: fix timeout error: invalid parameter "timeout": cannot parse "1.0s" to a valid duration
                    # timeout=float(self.config.timeout),
                )

                result = self.client.query_range(query_config.query, options)

                for metric in result.metrics:
                    org_id = metric.identifier.labels.get(
                        query_config.organization_identifier
                    )
                    if not org_id:
                        logger.error(
                            "Organization identifier label missing from metric. Please check the configuration.",
                            metric_name=metric.identifier.name,
                            metric_labels=list(metric.identifier.labels.keys()),
                            expected_label=query_config.organization_identifier,
                        )
                        sys.exit(1)

                    for sample in metric.samples:
                        org_id = metric.identifier.labels.get(
                            query_config.organization_identifier
                        )
                        remaining_labels = {
                            k: v
                            for k, v in metric.identifier.labels.items()
                            if k != query_config.organization_identifier
                        }

                        yield {
                            "organization_id": org_id,
                            "timestamp": sample.timestamp,
                            "value": sample.value,
                            "slaos_metric_name": query_config.slaos_metric_name,
                            "labels": remaining_labels,
                        }

            except Exception as e:
                logger.error(
                    "Failed to execute Prometheus query",
                    exception_type=type(e).__name__,
                    query=query_config.query,
                    error=str(e),
                )
                raise type(e)(str(e))

    def query_logs(self, start_time: int, end_time: int) -> Iterator[Dict[str, Any]]:
        raise NotImplementedError(
            "Querying logs is not supported by the Prometheus client"
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()
