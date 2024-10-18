import pytest
from unittest.mock import patch, MagicMock
from google.cloud import storage  # type: ignore
from google.auth.credentials import Credentials  # type: ignore

from src.clients.google import GoogleClient
from src.config.models.inputs.google import GoogleConfig, StorageConfig


@pytest.fixture
def mock_credentials():
    mock_creds = MagicMock(spec=Credentials)
    mock_creds.universe_domain = "googleapis.com"  # Set the universe_domain
    return mock_creds


@pytest.fixture
def mock_storage_client():
    return MagicMock(spec=storage.Client)


@pytest.fixture
def gcp_config():
    return GoogleConfig(
        project_id="test-project",
        auth_method="SERVICE_ACCOUNT",
        credentials_path="/path/to/credentials.json",
        storage_config=StorageConfig(bucket_name="test-bucket", prefix=None),
    )


def test_init_with_service_account(gcp_config, mock_credentials):
    with (
        patch(
            "google.oauth2.service_account.Credentials.from_service_account_file",
            return_value=mock_credentials,
        ),
        patch(
            "google.cloud.storage.Client", return_value=MagicMock(spec=storage.Client)
        ) as mock_storage_client,
    ):
        client = GoogleClient(gcp_config)

        # Assert that the GoogleClient was initialized correctly
        assert client.config == gcp_config

        # Assert that the storage Client was created with the correct arguments
        mock_storage_client.assert_called_once_with(
            project=gcp_config.project_id, credentials=mock_credentials
        )


def test_get_client_service_account(gcp_config, mock_credentials):
    with (
        patch(
            "google.oauth2.service_account.Credentials.from_service_account_file",
            return_value=mock_credentials,
        ) as mock_cred_func,
        patch("google.cloud.storage.Client") as mock_client_class,
    ):
        client = GoogleClient(gcp_config)

        mock_cred_func.assert_called_once_with(
            gcp_config.credentials_path,
        )
        mock_client_class.assert_called_once_with(
            project=gcp_config.project_id, credentials=mock_credentials
        )
        assert client.storage_client == mock_client_class.return_value
