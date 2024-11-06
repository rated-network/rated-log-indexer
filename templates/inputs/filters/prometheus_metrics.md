# slaOS Metrics Configuration Guide

This guide explains how to configure the metrics parsing section in your slaOS indexer using the MetricPattern model.

## Configuration Structure

Basic configuration example:
```yaml
version: 1
fields:
  - key: "user_id"
    hash: true
  - key: "region"
    hash: true
```

## Field Definitions

Each metric field supports the following attributes:

### Required Attributes
- `key`: String identifier for the metric field

### Optional Attributes
- `hash`: Boolean for enabling hashing

#### Hashing Implementation

slaOS uses SHA-256 (SHA-2 family) for field hashing, implemented as follows:

```python
sha256(str(value).encode()).hexdigest()
```

## Field Type Explanations

Fields are treated differently based on their content and configuration:

- **Standard Fields**: Values are stored as-is (e.g., response_time)
- **Hashed Fields**: Values are transformed using SHA-256 before storage
- **Numeric Fields**: Preserved as numbers for mathematical operations
- **String Fields**: Maintained as strings with optional hashing

## Best Practices

1. Hash sensitive information like user IDs, emails, or session tokens
2. Keep metrics fields unhashed for analysis (latency, counts, etc.)
3. Use consistent naming conventions for fields

## Parsing Process

1. The Rated Indexer applies the defined filters to each metric entry.
2. It extracts Prometheus labels (keys and values) and hashes any that have been identified as sensitive in the config, including the `organization_identifier`.
3. The parsed data is then ready for submission to slaOS.

## Notes

- Hashed fields cannot be reversed to original values
- Hashing is deterministic (same input = same hash)
- All values are converted to strings before hashing
