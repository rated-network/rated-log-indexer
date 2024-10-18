import json
import re
from enum import Enum

import structlog
from google.cloud import storage  # type: ignore
from google.oauth2 import service_account  # type: ignore
from typing import Dict, Any, Optional, Iterator
from pydantic import PositiveInt
import stamina

from src.config.models.inputs.google import GoogleConfig
from src.utils.time_conversion import from_milliseconds

logger = structlog.get_logger(__name__)


class ClientType(str, Enum):
    STORAGE = "storage"


class GoogleInputs(str, Enum):
    OBJECTS = "objects"


class GoogleClientError(Exception):
    """Custom exception for GCP Client errors."""

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class GoogleClient:

    def __init__(self, config: GoogleConfig, limit: Optional[PositiveInt] = None):
        self.config = config
        self.storage_config = config.storage_config
        self.storage_client = self._get_client(ClientType.STORAGE)
        self.bucket_name = (
            self.storage_config.bucket_name if self.storage_config else None
        )
        self.bucket = (
            self.storage_client.bucket(
                self.bucket_name, user_project=self.config.project_id
            )
            if self.bucket_name
            else None
        )
        self.log_features = (
            self.storage_config.log_features if self.storage_config else None
        )

    def _get_credentials(self):
        credentials = service_account.Credentials.from_service_account_file(
            self.config.credentials_path,
        )
        return credentials

    def _get_client(self, client_type: ClientType) -> storage.Client:
        if client_type == ClientType.STORAGE:
            return storage.Client(
                project=self.config.project_id,
                credentials=self._get_credentials(),
            )
        else:
            raise ValueError(f"Unsupported client type: {client_type}")

    @stamina.retry(on=GoogleClientError)
    def make_api_call(self) -> Any:
        try:
            return self.bucket.list_blobs()
        except Exception as e:
            msg = f"Unexpected error querying GCP: {str(e)}"
            logger.error(msg, exc_info=True)
            raise GoogleClientError(msg) from e

    def query_objects(
        self, start_time: PositiveInt, end_time: PositiveInt
    ) -> Iterator[Dict[str, Any]]:
        total_rows = 0
        blobs = self.make_api_call()
        start_date = from_milliseconds(start_time)
        end_date = from_milliseconds(end_time)

        for blob in blobs:

            # Check if the blob was created within the specified time range
            if start_date <= blob.time_created <= end_date:
                try:
                    logger.debug(f"Processing blob: {blob.name}")
                    yield from self._process_blob_content(blob)
                    total_rows += 1
                except Exception as e:
                    msg = f"Error processing blob {blob.name}: {str(e)}"
                    logger.error(msg, exc_info=True)
                    raise GoogleClientError(msg) from e

        logger.info(
            f"Processed {total_rows} blobs from GCP Storage",
            start_time=start_time,
            end_time=end_time,
            bucket_name=self.bucket_name if self.bucket_name else "N/A",
        )

    def _process_blob_content(self, blob: storage.Blob) -> Iterator[Dict[str, Any]]:

        if not self.log_features:
            msg = "Log features not provided in the config"
            logger.error(msg)
            raise GoogleClientError(msg)

        row_count = 0
        stream = blob.download_as_text().splitlines()
        for line_number, line in enumerate(stream, 1):
            # Split the line into individual JSON objects
            json_objects = re.findall(r"\{[^}]+}", line)

            for json_str in json_objects:
                try:
                    content = json.loads(json_str)
                    row = {
                        "content": content,
                        "id": content.get(self.log_features.id),
                        "timestamp": content.get(self.log_features.timestamp),
                        "_blob_name": blob.name,
                        "_line_number": line_number,
                        "_row_number": row_count,
                    }
                    yield row
                    row_count += 1
                except json.JSONDecodeError as e:
                    error_msg = (
                        f"JSON decode error in {blob.name}, line {line_number}, object {row_count}. "
                        f"Error: {str(e)}. "
                        f"Problematic content: {json_str[:200]}..."
                    )
                    logger.error(error_msg)
                    raise GoogleClientError(error_msg) from e

        logger.debug(f"Processed {row_count} objects from blob {blob.name}")

    def query_logs(
        self, start_time: PositiveInt, end_time: PositiveInt
    ) -> Iterator[Dict[str, Any]]:
        raise NotImplementedError(
            "Querying logs is not supported for Google Cloud Storage"
        )

    def query_metrics(
        self, start_time: PositiveInt, end_time: PositiveInt
    ) -> Iterator[Dict[str, Any]]:
        raise NotImplementedError(
            "Querying metrics is not supported for Google Cloud Storage"
        )
