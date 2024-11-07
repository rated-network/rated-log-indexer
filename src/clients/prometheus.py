import sys
from typing import Any, Dict, Iterator, Optional

import structlog
from rated_exporter_sdk.providers.prometheus.client import PrometheusClient  # type: ignore
from rated_exporter_sdk.providers.prometheus.types import PrometheusQueryOptions  # type: ignore
from rated_exporter_sdk.providers.prometheus.auth import PrometheusAuth  # type: ignore
from rated_exporter_sdk.providers.prometheus.managed.gcloud_auth import GCPPrometheusAuth  # type: ignore

from src.config.models.inputs.prometheus import PrometheusConfig
from src.utils.time_conversion import from_milliseconds

logger = structlog.get_logger(__name__)


class PrometheusClientWrapper:
    """Wrapper for the Prometheus SDK client to match the interface expected by the indexer."""

    def __init__(self, config: PrometheusConfig):
        self.config = config
        self.client = PrometheusClient(
            base_url=str(config.base_url),
            auth=self.create_auth(),
            timeout=config.timeout,
            max_retries=config.max_retries,
            retry_backoff_factor=config.retry_backoff_factor,
            pool_connections=config.pool_connections,
            pool_maxsize=config.pool_maxsize,
            max_parallel_queries=config.max_parallel_queries,
        )

    def create_auth(self) -> Optional[PrometheusAuth]:
        """Create the appropriate authentication object based on the configuration."""
        auth_config = self.config.auth
        if not auth_config:
            return None
        try:
            if auth_config.username and auth_config.password:
                return PrometheusAuth(
                    username=auth_config.username, password=auth_config.password
                )
            elif auth_config.token:
                return PrometheusAuth(token=auth_config.token)
            elif auth_config.cert_path and auth_config.key_path:
                return PrometheusAuth(ca_cert=auth_config.cert_path)
            elif (
                auth_config.gcloud_service_account_path
                and auth_config.gcloud_target_principal
            ):
                return GCPPrometheusAuth(
                    service_account_file=auth_config.gcloud_service_account_path,
                    target_principal=auth_config.gcloud_target_principal,
                )
            return None
        except Exception as e:
            logger.error(f"Failed to create auth: {str(e)}")
            return None

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
                    org_id = metric.identifier.labels.get(
                        query_config.organization_identifier,
                        query_config.fallback_org_id,
                    )
                    if not org_id:
                        logger.error(
                            "Organization identifier label missing from metric and no `fallback_org_id` provided. Please check the configuration.",
                            query=query_config.query,
                            metric_labels=list(metric.identifier.labels.keys()),
                            expected_label=query_config.organization_identifier,
                            fallback_org_id=query_config.fallback_org_id,
                        )
                        sys.exit(1)

                    remaining_labels = {
                        k: v
                        for k, v in metric.identifier.labels.items()
                        if k != query_config.organization_identifier
                    }

                    for sample in metric.samples:
                        yield {
                            "organization_identifier": query_config.organization_identifier,
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
