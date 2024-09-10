# Datadog Basic Log Ingestion

This guide explains how to configure the slaOS indexer for basic log ingestion from Datadog.

## Configuration Example

```yaml
input:
  type: datadog
  datadog:
    site: datadoghq.com
    api_key: your_datadog_api_key
    app_key: your_datadog_app_key
    logs_config:
      indexes: ["main", "prod"]
      query: "service:my-app status:error"
```

## Field Explanations

### Datadog Config

- `site`: The Datadog site to use (e.g., datadoghq.com, datadoghq.eu).
- `api_key`: Your Datadog API key.
- `app_key`: Your Datadog application key.


You need the following scopes for the API: `events_read`.

### Logs Config

- `indexes`: List of log indexes to query. Use ["*"] for all indexes.
- `query`: The search query to filter logs.

## Usage Notes

1. Ensure your Datadog API and application keys have the necessary permissions to read logs.
2. The `query` field uses Datadog's log query syntax. Refer to Datadog's documentation for advanced query options.
3. Adjust the `indexes` list to focus on specific log indexes if needed.
4. For production use, consider using environment variables or a secure secret management system for the API and app keys.
