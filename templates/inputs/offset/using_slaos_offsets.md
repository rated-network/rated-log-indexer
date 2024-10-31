# Using slaOS Offsets in slaOS

This guide explains how to configure slaOS offsets for your slaOS indexer.

## Example Configuration

```yaml
offset:
  type: slaos
  override_start_from: false
  start_from: 1633046400000
  start_from_type: bigint
  slaos:
    ingestion_id: b92d6662-6bd3-4b1f-b2d2-e6880cbe43ee
    ingestion_key: secret-key
    ingestion_url: https://api.rated.co/v1/ingest
    datastream_key: stream-name

```

## Field Explanations

- `type`: Set to `slaos` to use the slaOS API for offset retrieval.

- `override_start_from`:
  - `true`: The indexer will start from the specified `start_from` value.
  - `false`: The indexer will use the last recorded offset, if available.

- `start_from`: The initial offset value. This must be a Unix timestamp in milliseconds.

- `start_from_type`: Always set to `bigint` to accommodate Unix timestamps in milliseconds.

- `slaos`: Configuration for the slaOS API connection:
  - `ingestion_id`: Your unique ingestion identifier provided by Rated.
  - `ingestion_key`: Your secret ingestion key for authentication.
  - `ingestion_url`: The URL of the Rated API ingestion endpoint.
  - `datastream_key`: The key of the SLI datastream.