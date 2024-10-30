## Inputs Configuration

The `inputs` section defines the data sources for your indexer. It is a list of integration objects, allowing you to run multiple inputs/integrations concurrently, fully managed.

### Basic Structure

```yaml
inputs:
  - integration: <integration_type>
    slaos_key: <unique_identifier>
    type: <logs_or_metrics>
    <integration_specific_config>
    filters: <filter_config>
    offset: <offset_config>
```

### Key Components

1. **integration**: Specifies the data source (e.g., cloudwatch, datadog). This determines which integration-specific configuration is required.

2. **slaos_key**: A unique identifier for the input. This is used to differentiate data submitted to slaOS when multiple integrations are running.
   - Example: If `slaos_key` is set to "prod_api_cloudwatch", a data point with key "status_code" will be submitted to slaOS as "prod_api_cloudwatch_status_code".
   - Validation: Each `slaos_key` must be unique across all inputs to avoid conflicts.
   - Context: The `slaos_key` is mandatory when using more than one integration. It prevents conflicts in data submitted to slaOS by prefixing all data points from this input with the specified prefix.

3. **type**: Specifies "logs" or "metrics". This determines how the input data is processed and which additional configurations (like filters) are required.

4. **filters**: Configuration for data filtering. This is only applicable and required for log-type inputs. It defines how log data should be parsed and transformed.

5. **offset**: Configuration for tracking the last processed position in the data stream. This ensures idempotent operation and allows for efficient data processing, especially after interruptions or for backfills.

### Filters Section

The `filters` section defines how the indexer processes and transforms input data. This is where you specify the log format and define the fields you want to extract. It is only applicable for log-type inputs and is not needed for metrics.

For detailed information and examples of filter configurations, please refer to the [Filters Documentation](./filters).

### Offset Section

The `offset` section is responsible for tracking the last processed position in the input data stream. This ensures idempotent operation and allows for efficient data processing.

For detailed information and examples of offset configurations, please refer to the [Offset Documentation](./offset/).

## Integration-Specific Configuration

Each integration type (e.g., CloudWatch, Datadog) has its own specific configuration requirements. These are defined within the input configuration under the integration name.

For detailed information on configuring specific integrations, including log and metric ingestion configs, please refer to the [Clients Documentation](./clients/).

## Output Configuration

The `output` section defines where the processed data should be sent. For detailed information and examples of output configurations, please refer to the [Output Documentation](./output/).

## Best Practices

1. Use unique `slaos_key` values for each input to avoid data conflicts.
2. Regularly review and update your configuration to ensure it aligns with your current monitoring needs.
3. Use the `override_start_from` option in the offset configuration for backfills or when you need to reprocess data from a specific point.
4. Refer to our [GitHub repository](https://github.com/rated-network/rated-log-indexer/tree/main/templates) for the latest configuration templates and examples.
