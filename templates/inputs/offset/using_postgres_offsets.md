# Using PostgreSQL Offsets in slaOS

This guide explains how to configure PostgreSQL offsets for your slaOS indexer.

## Example Configuration

```yaml
offset:
  type: postgres
  override_start_from: true
  start_from: 1633046400000
  start_from_type: bigint
  postgres:
    table_name: offset_tracking
    host: localhost
    port: 5432
    database: mydb
    user: myuser
    password: mypassword
```

## Field Explanations

- `type`: Set to `postgres` to use PostgreSQL for offset storage.

- `override_start_from`:
  - `true`: The indexer will start from the specified `start_from` value.
  - `false`: The indexer will use the last recorded offset, if available.

- `start_from`: The initial offset value. This must be a Unix timestamp in milliseconds.

- `start_from_type`: Always set to `bigint` to accommodate Unix timestamps in milliseconds.

- `postgres`: Configuration for the PostgreSQL connection:
  - `table_name`: The name of the table where offsets will be stored.
  - `host`: The hostname of your PostgreSQL server.
  - `port`: The port number for your PostgreSQL server (default is 5432).
  - `database`: The name of the database to connect to.
  - `user`: The username for database authentication.
  - `password`: The password for database authentication.

## Best Practices

1. Ensure your PostgreSQL server is properly secured and accessible only to authorized systems.
2. Use a dedicated database user with minimal permissions for offset tracking.
3. Regularly backup your offset tracking table to prevent data loss.
4. Monitor the performance of your PostgreSQL server, especially if tracking offsets for multiple inputs.
