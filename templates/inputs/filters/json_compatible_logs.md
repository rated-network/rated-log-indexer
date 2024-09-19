# JSON-Compatible Logs Configuration in slaOS

This guide explains how to configure the filters section for JSON-compatible logs in your slaOS indexer.

## Log Formats

slaOS supports both structured (JSON) and unstructured (raw text) log formats. This guide focuses on JSON-compatible logs. Note that unstructured logs are processed using regex to extract relevant features.

## Example Configuration

```yaml
filters:
  version: 1
  log_format: json_dict
  log_example: {"timestamp": "2023-09-19T14:30:00Z", "level": "INFO", "message": "User logged in", "user_id": "12345", "request_id": "abc-123", "response_time_ms": 150}
  fields:
    - key: "timestamp"
      path: "timestamp"
      field_type: "timestamp"
      format: "%Y-%m-%dT%H:%M:%SZ"
    - key: "level"
      path: "level"
      field_type: "string"
    - key: "message"
      path: "message"
      field_type: "string"
    - key: "user_id"
      path: "user_id"
      field_type: "string"
    - key: "request_id"
      path: "request_id"
      field_type: "string"
    - key: "response_time_ms"
      path: "response_time_ms"
      field_type: "integer"
```

## Field Explanations

- `version`: The version of the filter configuration schema. Currently, this should always be set to `1`.

- `log_format`: For JSON logs, this should be set to `json_dict`.

- `log_example`: A sample log entry in JSON format. This helps in understanding the structure of your logs and aids in configuring the fields correctly.

- `fields`: An array of field configurations. Each field configuration consists of:
  - `key`: The name you want to give to this field in slaOS.
  - `path`: The JSON path to the field in your log entry. For top-level fields, this is usually the same as the key in your JSON log.
  - `field_type`: The data type of the field. Supported types include:
    - `string`: For text data
    - `integer`: For whole numbers
    - `float`: For decimal numbers
    - `timestamp`: For date/time fields (must be in a format parseable by slaOS)
  - `format`: Required for `timestamp` fields. Specifies the date/time format string.

## Field Types

slaOS supports the following field types:
- `timestamp`: For date and time information
- `integer`: For whole numbers
- `float`: For decimal numbers
- `string`: For text data

## Timestamp Formats

When defining timestamp fields, it's crucial to use the correct datetime format string that matches the format of your log timestamps. For example:
- `"%Y-%m-%dT%H:%M:%SZ"`: For ISO 8601 format (e.g., "2023-09-19T14:30:00Z")
- `"%Y-%m-%d %H:%M:%S"`: For simple date-time format (e.g., "2023-09-19 14:30:00")

## Best Practices

1. Ensure that your `log_example` accurately represents your actual log structure.
2. Include all fields that you might want to use in your SLIs or for analysis.
3. Pay attention to the `field_type` - using the correct type will ensure proper handling and querying of your data.
4. For nested JSON structures, use dot notation in the `path`. For example, if you have a nested field like `{"user": {"id": "12345"}}`, the path would be `user.id`.

> We don't support arrays. Right now, `errors[0]` would be ignored.


## Parsing Process

1. slaOS applies the defined filters to each log entry.
2. It extracts the specified fields according to their paths.
3. The extracted data is converted to the specified field types.
4. For timestamp fields, the provided format is used to parse the date/time string.
5. The parsed data is then ready for further processing, analysis, or integration into your monitoring tools.
