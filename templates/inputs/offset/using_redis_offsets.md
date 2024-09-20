# Using Redis Offsets in slaOS

This guide explains how to configure Redis offsets for your slaOS indexer.

## Example Configuration

```yaml
offset:
  type: redis
  override_start_from: true
  start_from: 1633046400000
  start_from_type: bigint
  redis:
    host: localhost
    port: 6379
    db: 0
    password: mypassword
```

## Field Explanations

- `type`: Set to `redis` to use Redis for offset storage.

- `override_start_from`:
  - `true`: The indexer will start from the specified `start_from` value.
  - `false`: The indexer will use the last recorded offset, if available.

- `start_from`: The initial offset value. This must be a Unix timestamp in milliseconds.

- `start_from_type`: Always set to `bigint` to accommodate Unix timestamps in milliseconds.

- `redis`: Configuration for the Redis connection:
  - `host`: The hostname of your Redis server.
  - `port`: The port number for your Redis server (default is 6379).
  - `db`: The Redis database number to use (default is 0).
  - `password`: The password for Redis authentication (if required).

## Best Practices

1. Ensure your Redis server is properly secured and accessible only to authorized systems.
2. Use a dedicated Redis database (DB number) for offset tracking to isolate it from other applications.
3. Enable Redis persistence (AOF or RDB) to prevent data loss in case of server restarts.
4. Monitor the memory usage of your Redis server, especially if tracking offsets for multiple inputs.
5. Consider using Redis Sentinel or Redis Cluster for high availability setups.
