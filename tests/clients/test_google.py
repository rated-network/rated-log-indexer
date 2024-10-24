import pytest
from datetime import datetime, timezone
from unittest.mock import Mock, patch
from google.cloud import storage  # type: ignore

from src.clients.google import GoogleClientError, GoogleClient
from src.config.models.inputs.google import (
    GoogleConfig,
    StorageInputs,
    GoogleInputs,
    StorageConfig,
    GoogleLogsConfig,
    LogFeatures,
    AuthMethod,
)


@pytest.fixture
def mock_blob():
    blob = Mock(spec=storage.Blob)
    blob.name = "test-blob-2024.json"
    blob.time_created = datetime(2024, 1, 1, tzinfo=timezone.utc)
    return blob


@pytest.fixture
def mock_storage_client():
    with patch("google.cloud.storage.Client") as mock_client:
        yield mock_client


@pytest.fixture
def mock_credentials():
    with patch(
        "google.oauth2.service_account.Credentials.from_service_account_file"
    ) as mock_creds:
        yield mock_creds


@pytest.fixture
def basic_config():
    return GoogleConfig(
        project_id="test-project",
        auth_method=AuthMethod.SERVICE_ACCOUNT,
        credentials_path="/path/to/credentials.json",
        config_type=GoogleInputs.OBJECTS,
        storage_config=StorageConfig(
            bucket_name="test-bucket",
            prefix="test-prefix",
            input_type=StorageInputs.LOGS,
            logs_config=GoogleLogsConfig(
                log_name="test-log",
                log_features=LogFeatures(id="log_id", timestamp="timestamp"),
            ),
        ),
    )


@pytest.fixture
def google_client(mock_storage_client, mock_credentials, basic_config):
    return GoogleClient(config=basic_config)


@pytest.fixture
def mock_blob_with_name():
    def _create_blob(name: str):
        blob = Mock(spec=storage.Blob)
        blob.name = name
        blob.time_created = datetime(2024, 1, 1, tzinfo=timezone.utc)
        return blob

    return _create_blob


def test_init(google_client, basic_config):
    assert google_client.config == basic_config
    assert google_client.input_type == GoogleInputs.OBJECTS
    assert google_client.storage_config is not None
    assert google_client.storage_config.bucket_name == "test-bucket"


def test_get_params(google_client):
    params = google_client._get_params(GoogleInputs.OBJECTS)
    assert params == (["test-bucket"], {"prefix": "test-prefix"})


def test_get_params_invalid_type(google_client):
    with pytest.raises(ValueError):
        google_client._get_client("invalid_type")  # type: ignore


def test_default_file_type_validation(google_client, mock_blob_with_name):
    # Test valid extensions
    for ext in [".json", ".jsonl", ".log"]:
        blob = mock_blob_with_name(f"test{ext}")
        google_client._validate_file_type(blob)  # Should not raise


@pytest.mark.parametrize(
    "blob_content,expected_rows",
    [
        (
            [
                '{"log_id": "123", "timestamp": "2024-01-01T00:00:00Z", "message": "test"}'
            ],
            1,
        ),
        (
            [
                '{"log_id": "123", "timestamp": "2024-01-01T00:00:00Z", "message": "test1"}',
                '{"log_id": "124", "timestamp": "2024-01-01T00:00:01Z", "message": "test2"}',
            ],
            2,
        ),
    ],
)
def test_query_objects_success(google_client, mock_blob, blob_content, expected_rows):
    # Setup mock blob content
    mock_blob.download_as_text.return_value = "\n".join(blob_content)

    # Setup mock list_blobs
    google_client.storage_client.list_blobs.return_value = [mock_blob]

    # Query objects
    start_time = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    end_time = int(datetime(2024, 1, 2, tzinfo=timezone.utc).timestamp() * 1000)

    results = list(google_client.query_objects(start_time, end_time))

    print(results)

    assert len(results) == expected_rows
    for result in results:
        assert "id" in result
        assert "timestamp" in result
        assert "_blob_name" in result
        assert "_line_number" in result
        assert "_row_number" in result
        assert "content" in result


def test_query_objects_missing_required_fields(google_client, mock_blob):
    # Setup mock blob with missing required fields
    mock_blob.download_as_text.return_value = (
        '{"wrong_id": "123", "wrong_timestamp": "2024-01-01T00:00:00Z"}'
    )
    google_client.storage_client.list_blobs.return_value = [mock_blob]

    start_time = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    end_time = int(datetime(2024, 1, 2, tzinfo=timezone.utc).timestamp() * 1000)

    with pytest.raises(GoogleClientError) as exc_info:
        list(google_client.query_objects(start_time, end_time))

    error_message = str(exc_info.value)
    assert "Missing required fields" in error_message
    assert "wrong_id" in error_message
    assert "wrong_timestamp" in error_message


def test_query_objects_invalid_json(google_client, mock_blob):
    # Setup mock blob with invalid JSON
    mock_blob.download_as_text.return_value = "invalid json content"
    google_client.storage_client.list_blobs.return_value = [mock_blob]

    start_time = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    end_time = int(datetime(2024, 1, 2, tzinfo=timezone.utc).timestamp() * 1000)

    with pytest.raises(GoogleClientError) as exc_info:
        list(google_client.query_objects(start_time, end_time))
    assert "JSON decode error" in str(exc_info.value)


def test_query_objects_time_filter(google_client, mock_blob):
    # Setup mock blob with timestamp outside range
    mock_blob.time_created = datetime(
        2023, 1, 1, tzinfo=timezone.utc
    )  # Before our query range
    mock_blob.download_as_text.return_value = (
        '{"log_id": "123", "timestamp": "2024-01-01T00:00:00Z"}'
    )
    google_client.storage_client.list_blobs.return_value = [mock_blob]

    start_time = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    end_time = int(datetime(2024, 1, 2, tzinfo=timezone.utc).timestamp() * 1000)

    results = list(google_client.query_objects(start_time, end_time))
    assert len(results) == 0  # Should filter out the blob due to timestamp


def test_query_objects_unsupported_storage_type(google_client, mock_blob):
    # Change storage input type to something unsupported
    google_client.storage_input = "METRICS"  # Not StorageInputs.LOGS

    mock_blob.download_as_text.return_value = (
        '{"log_id": "123", "timestamp": "2024-01-01T00:00:00Z"}'
    )
    google_client.storage_client.list_blobs.return_value = [mock_blob]

    start_time = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    end_time = int(datetime(2024, 1, 2, tzinfo=timezone.utc).timestamp() * 1000)

    with pytest.raises(GoogleClientError) as exc_info:
        list(google_client.query_objects(start_time, end_time))
    assert "Unsupported storage config type" in str(exc_info.value)
