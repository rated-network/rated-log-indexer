# Rated API Output

This guide explains how to configure the slaOS indexer to send processed data to the Rated API.

## Configuration Example

```yaml
output:
  type: rated
  rated:
    ingestion_id: your_ingestion_id
    ingestion_key: your_ingestion_key
    ingestion_url: https://api.rated.network/v1/ingest
```

## Field Explanations

### Output Config

- `type`: Set to `rated` to enable Rated API output.

### Rated Output Config

- `ingestion_id`: Your unique ingestion identifier provided by Rated.
- `ingestion_key`: Your secret ingestion key for authentication.
- `ingestion_url`: The URL of the Rated API ingestion endpoint.

## Usage Notes

1. Ensure that you have valid credentials (`ingestion_id` and `ingestion_key`) from Rated dashboard (https://app.rated.network).
2. The `ingestion_url` should be the correct endpoint for your Rated API integration.
3. This output type is suitable for production use, as it sends data directly to the Rated platform for analysis.
4. It will batch and send processed data to the Rated API: 50 records or 10s, whichever comes first.

## Example Use Case

When you want to send your processed data to the Rated platform for analysis and visualization:

```yaml
output:
  type: rated
  rated:
    ingestion_id: abc123def456
    ingestion_key: secret_key_789xyz
    ingestion_url: https://api.rated.network/v1/ingest
```

This configuration will send all processed data to your Rated account, where you can perform further analysis and create visualizations.
