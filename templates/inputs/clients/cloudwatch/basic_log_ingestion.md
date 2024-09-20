# CloudWatch Basic Log Ingestion

This guide explains how to configure the slaOS indexer for basic log ingestion from AWS CloudWatch.

## Configuration Example

```yaml
inputs:
  - integration: cloudwatch
    integration_prefix: cloudwatch_logs
    type: logs
    cloudwatch:
      region: us-east-1
      aws_access_key_id: AKIAXXXXXX
      aws_secret_access_key: XXXX+XXXX/XXXX/XXXX
      logs_config:
        log_group_name: /aws/apprunner/api/XXXXX/application
        # log_stream_name: XXXXX
        filter_pattern: "{ $.level = \"ERROR\" }"
    filters: <filter_config>
    offset: <offset_config>
```

## Field Explanations

### CloudWatch Config

- `region`: AWS region where your CloudWatch logs are located.
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
                "logs:DescribeLogGroups",
                "logs:GetLogEvents"
            ],
            "Resource": "*"
        }
    ]
}
```

### Logs Config

- `log_group_name`: The full name of the CloudWatch log group to ingest.
- `log_stream_name` (Optional): Specific log stream within the log group.
- `filter_pattern` (Optional): CloudWatch Logs Insights query to filter logs. For more information, see [Filter and Pattern Syntax](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/FilterAndPatternSyntax.html).

## Usage Notes

1. Ensure your AWS credentials have the necessary permissions to read from the specified CloudWatch log group.
2. The `filter_pattern` can be used to focus on specific log entries, reducing data transfer and processing.
3. If `log_stream_name` is not specified, all streams in the log group will be ingested.
4. For production, consider using IAM roles or AWS Secrets Manager instead of hardcoding credentials. See `secrets` in the [using secrets manager](../templates/secrets/using_aws_secrets_manager.md) for more information.
