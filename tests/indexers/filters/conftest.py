from datetime import datetime, timezone

import pytest

from src.indexers.filters.types import MetricEntry


@pytest.fixture
def test_metrics():
    return [
        MetricEntry(
            metric_name="cpu_usage",
            value=75.5,
            organization_id="org_1",
            event_timestamp=datetime(2024, 10, 31, 12, 0, 0, tzinfo=timezone.utc),
            labels={
                "environment": "prod",
                "service": "api",
                "region": "us-east",
                "instance": "instance-1",
                "version": "v1.0",
            },
        ),
        MetricEntry(
            metric_name="memory_usage_mb",
            value=1024.0,
            organization_id="org_2",
            event_timestamp=datetime(2024, 10, 31, 12, 1, 0, tzinfo=timezone.utc),
            labels={
                "environment": "staging",
                "service": "web",
                "region": "us-west",
                "instance": "instance-2",
                "version": "v1.1",
            },
        ),
        MetricEntry(
            metric_name="request_latency_ms",
            value=250.3,
            organization_id="org_1",
            event_timestamp=datetime(2024, 10, 31, 12, 2, 0, tzinfo=timezone.utc),
            labels={
                "environment": "dev",
                "service": "worker",
                "region": "eu-central",
                "instance": "instance-3",
                "version": "v1.2",
            },
        ),
        MetricEntry(
            metric_name="error_count",
            value=0.0,
            organization_id="org_3",
            event_timestamp=datetime(2024, 10, 31, 12, 3, 0, tzinfo=timezone.utc),
            labels={
                "environment": "prod",
                "service": "api",
                "region": "us-west",
                "instance": "instance-4",
                "version": "v2.0",
            },
        ),
        MetricEntry(
            metric_name="disk_usage_percent",
            value=85.5,
            organization_id="org_2",
            event_timestamp=datetime(2024, 10, 31, 12, 4, 0, tzinfo=timezone.utc),
            labels={
                "environment": "staging",
                "service": "worker",
                "region": "eu-central",
                "instance": "instance-5",
                "version": "v1.0",
            },
        ),
    ]
