# Using AWS Secrets Manager

This guide explains how to configure the slaOS indexer to use AWS Secrets Manager for secure handling of sensitive information.

## Configuration Examples

### Using AWS Secrets Manager

```yaml
secrets:
  use_secrets_manager: true
  provider: aws
  aws:
    region: us-east-1
    aws_access_key_id: AKIAXXXX
    aws_secret_access_key: XXXX/XXXX/XXXX
```

### Not Using Secrets Manager

```yaml
secrets:
  use_secrets_manager: false
```

## Field Explanations

- `use_secrets_manager`: Enable/disable secrets manager.
- `provider`: Secrets manager provider (only `aws` supported).
- `region`: AWS region for Secrets Manager.
- `aws_access_key_id`: AWS access key ID.
- `aws_secret_access_key`: AWS secret access key.

## AWS Secrets Manager Usage

1. Ensure AWS credentials have necessary permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": ["secretsmanager:GetSecretValue"],
            "Resource": "arn:aws:secretsmanager:us-east-1:123456789012:secret:slaos-prod-credentials-AbCdEf"
        }
    ]
}
```

2. Store sensitive information in AWS Secrets Manager.
3. The indexer retrieves secrets during runtime.

## Secret Resolution

Reference secrets in your configuration using one of the following formats:

### For String Secrets

```
secret:secrets_manager_id
```

Example:
```yaml
input:
  type: datadog
  datadog:
    api_key: secret:datadog-api-key_secrets_manager_id
    app_key: secret:datadog-app-key_secrets_manager_id
```

### For Dictionary Secrets

```
secret|key_name:secrets_manager_id
```

Example:
```yaml
input:
  type: datadog
  datadog:
    credentials:
      api_key: secret|api_key:datadog-credentials_secrets_manager_id
      app_key: secret|app_key:datadog-credentials_secrets_manager_id
```

In this case, `datadog-credentials_secrets_manager_id` should contain a JSON object like:

```json
{
  "api_key": "your_api_key_here",
  "app_key": "your_app_key_here"
}
```

The indexer resolves these references using the configured AWS Secrets Manager, fetching either the entire string or the specified key from a dictionary secret.

## Without Secrets Manager

When `use_secrets_manager` is `false`, provide sensitive information directly in configuration files or environment variables. This is less secure and not recommended for production.

Example:
```yaml
secrets:
  use_secrets_manager: false

input:
  type: cloudwatch
  cloudwatch:
    aws_access_key_id: AKIAIOSFODNN7EXAMPLE
    aws_secret_access_key: wJaEXAMPLE/EXAMPLE/bPxRfiCYEXAMPLEKEY
```
