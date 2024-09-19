# Datadog Metrics Ingestion

This guide explains how to configure the slaOS indexer for metrics ingestion from Datadog.

## Configuration Example

```yaml
inputs:
  - integration: datadog
    integration_prefix: datadog_metrics
    type: metrics
    datadog:
      site: datadoghq.com
      api_key: your_datadog_api_key
      app_key: your_datadog_app_key
      metrics_config:
        metric_name: aws.lambda.invocations
        interval: 300
        statistic: avg
        customer_identifier: function_name
        metric_tag_data:
          - customer_value: my-lambda-function
            tag_string: "function_name:my-lambda-function,environment:prod"
          - customer_value: another-lambda-function
            tag_string: "function_name:another-lambda-function,environment:staging"
    offset: <offset_config>
```

## Field Explanations

### Datadog Config

- `site`: The Datadog site to use (e.g., datadoghq.com, datadoghq.eu).
- `api_key`: Your Datadog API key.
- `app_key`: Your Datadog application key.

You need the following scopes for the API: `events_read`.

### Metrics Config

- `metric_name`: Name of the metric to ingest.
- `interval`: The granularity, in seconds, of the returned datapoints.
- `statistic`: The statistic to use (avg, min, max, or sum).
- `customer_identifier`: The tag name used to identify different customers or entities.
- `metric_tag_data`: List of tag configurations for querying the metric.
  - `customer_value`: The value of the customer identifier tag.
  - `tag_string`: The full tag string for querying the metric.

## Usage Notes

1. The `statistic` must be one of the predefined `DatadogStatistic` enum values.
2. The `interval` will be automatically converted to milliseconds in the configuration.
3. Each `tag_string` in `metric_tag_data` must include the `customer_identifier` with its corresponding `customer_value`.
4. The indexer will automatically generate `metric_queries` based on the provided `metric_tag_data`.
5. Ensure your Datadog API and application keys have permissions to read the specified metrics.
