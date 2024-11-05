# Simple Prometheus Integration Example

## Basic Configuration
```yaml
inputs:
  - integration: prometheus
    slaos_key: prometheus_metrics
    type: metrics
    prometheus:
      base_url: "http://prometheus:9090"
      queries:
        - query: 'rate(http_request_duration_seconds_count{job="api"}[5m])'
          step: "1m"
          slaos_metric_name: "http_request_rate"
          organization_identifier: "customer_id"
      timeout: 15.0
      max_retries: 3
    offset:
      type: redis
      override_start_from: true
      start_from: 1724803200000
      start_from_type: bigint
      redis:
        host: redis
        port: 6379
        db: 0
```

## Query Configuration

### Basic Query Components
```yaml
queries:
  - query: '<promql_query>'                    # The PromQL query string
    step: "<duration>"                         # Time between data points
    slaos_metric_name: "<metric_name>"        # Name in slaOS
    organization_identifier: "<label_name>"    # Label for customer identification
```

### Query Examples
```yaml
queries:
  # Request Rate
  - query: 'rate(http_requests_total{job="api"}[5m])'
    step: "1m"
    slaos_metric_name: "request_rate"
    organization_identifier: "customer_id"

  # Error Rate
  - query: 'rate(http_errors_total{job="api"}[5m])'
    step: "1m"
    slaos_metric_name: "error_rate"
    organization_identifier: "customer_id"

  # 95th Percentile Latency
  - query: 'histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))'
    step: "1m"
    slaos_metric_name: "latency_p95"
    organization_identifier: "customer_id"
```

## Connection Settings

```yaml
prometheus:
  timeout: 15.0                # Request timeout in seconds
  pool_connections: 10         # Initial connection pool size
  pool_maxsize: 10            # Maximum connections
  max_parallel_queries: 5      # Concurrent query limit
  retry_backoff_factor: 0.1    # Retry delay multiplier
  max_retries: 3              # Maximum retry attempts
```

## Offset Configuration

Redis offset store for tracking metric collection progress:
```yaml
offset:
  type: redis
  override_start_from: true           # Start from specific timestamp
  start_from: 1724803200000          # Unix timestamp in milliseconds
  start_from_type: bigint            # Timestamp data type
  redis:
    host: redis                      # Redis host
    port: 6379                       # Redis port
    db: 0                            # Redis database number
```

## Query Best Practices

1. Time Ranges
```yaml
# Last 5 minutes of data
'rate(metric_name[5m])'

# With offset
'rate(metric_name[5m] offset 1m)'
```

2. Aggregation
```yaml
# Sum by customer
'sum by (customer_id) (rate(metric_name[5m]))'

# Average by service
'avg by (service) (rate(metric_name[5m]))'
```

3. Label Matching
```yaml
# Exact match
'{customer_id="customer1"}'

# Regex match
'{customer_id=~"customer.*"}'
```

## Common Use Cases

1. Request Monitoring
```yaml
queries:
  - query: 'sum by (customer_id) (rate(http_requests_total{job="api"}[5m]))'
    slaos_metric_name: "total_requests"
    organization_identifier: "customer_id"
```

2. Error Tracking
```yaml
queries:
  - query: 'sum by (customer_id) (rate(http_errors_total{job="api"}[5m])) / sum by (customer_id) (rate(http_requests_total{job="api"}[5m]))'
    slaos_metric_name: "error_rate"
    organization_identifier: "customer_id"
```

3. Performance Metrics
```yaml
queries:
  - query: 'histogram_quantile(0.95, sum by (le, customer_id) (rate(http_duration_seconds_bucket{job="api"}[5m])))'
    slaos_metric_name: "p95_latency"
    organization_identifier: "customer_id"
```

## Notes
- Queries must include the `organization_identifier` label
- Step parameter should align with collection frequency
- Use appropriate time windows for rate calculations
- Consider query performance impact
