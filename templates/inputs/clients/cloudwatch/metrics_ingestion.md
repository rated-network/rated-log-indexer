# CloudWatch Metrics Ingestion

This guide explains how to configure the slaOS indexer for metrics ingestion from AWS CloudWatch.

## Configuration Example

```yaml
inputs:
  - integration: cloudwatch
    slaos_key: cloudwatch_metrics
    type: metrics
    cloudwatch:
      region: us-east-1
      aws_access_key_id: AKIAXXXXXX
      aws_secret_access_key: XXXX+XXXX/XXXX/XXXX
      metrics_config:
        namespace: AWS/Lambda
        metric_name: Invocations
        period: 300
        statistic: Average
        organization_identifier: FunctionName
        metric_queries:
          - - name: FunctionName
              value: my-lambda-function
          - - name: FunctionName
              value: another-lambda-function
    offset: <offset_config>
```

## Field Explanations

### CloudWatch Config

- `region`: AWS region where your CloudWatch metrics are located.
- `aws_access_key_id`: AWS access key for authentication.
- `aws_secret_access_key`: AWS secret key for authentication.

You need the following permissions:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "cloudwatch:GetMetricData",
                "cloudwatch:GetMetricStatistics",
                "cloudwatch:ListMetrics"
            ],
            "Resource": "*"
        }
    ]
}
```

### Metrics Config

- `namespace`: The CloudWatch namespace for the metric.
- `metric_name`: Name of the metric to ingest.
- `period`: The granularity, in seconds, of the returned datapoints.
- `statistic`: The statistic to use (Average, Minimum, Maximum, Sum, or SampleCount).
- `organization_identifier`: The dimension name used to identify different customers or entities.
- `metric_queries`: List of dimension sets to query for the metric.

For more information on CloudWatch metrics, see [CloudWatch Metrics and Dimensions](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_concepts.html).

## Usage Notes

1. The `statistic` must be one of the predefined `CloudwatchStatistic` enum values.
2. Each item in `metric_queries` is a list of dimensions for a specific query.
3. The `organization_identifier` must be present in each set of dimensions in `metric_queries`.
4. Adjust the `period` based on your monitoring needs and CloudWatch metric resolution.
5. Ensure your AWS credentials have permissions to read the specified CloudWatch metrics.
