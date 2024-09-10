# Multi-Source Ingestion: CloudWatch and Datadog

This guide explains how to configure the slaOS indexer for ingesting both logs and metrics from AWS CloudWatch and Datadog simultaneously.

## Configuration Example

```yaml
input:
  - type: cloudwatch
    cloudwatch:
      region: us-east-1
      aws_access_key_id: AKIA6M4GOBIXNPPDFXHS
      aws_secret_access_key: LxAAkt22atEKjn98i+AGLqQf/O/GLme3zp9YboBc
      logs_config:
        log_group_name: /aws/lambda/my-function
        filter_pattern: "{ $.level = \"ERROR\" }"
      metrics_config:
        namespace: AWS/Lambda
        metric_name: Invocations
        period: 300
        statistic: Average
        customer_identifier: FunctionName
        metric_queries:
          - - name: FunctionName
              value: my-lambda-function

  - type: datadog
    datadog:
      site: datadoghq.com
      api_key: your_datadog_api_key
      app_key: your_datadog_app_key
      logs_config:
        indexes: ["main"]
        query: "service:my-app status:error"
      metrics_config:
        metric_name: aws.lambda.duration
        interval: 300
        statistic: avg
        customer_identifier: function_name
        metric_tag_data:
          - customer_value: my-lambda-function
            tag_string: "function_name:my-lambda-function,environment:prod"
```

## Usage Notes

1. Ensure all necessary credentials (AWS and Datadog) have the required permissions.
2. The configuration allows for ingesting both logs and metrics from each source.
3. You can adjust the specific metrics, log groups, and queries for each source as needed.
4. The indexer will process data from both sources concurrently.
5. Ensure your data processing pipeline can handle the combined volume of data from multiple sources.
6. Monitor the performance of your indexer when ingesting from multiple sources, and adjust resources as necessary.
