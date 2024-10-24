import json
from enum import Enum

import structlog
from google.cloud import storage  # type: ignore
from google.oauth2 import service_account  # type: ignore
from typing import Dict, Any, Optional, Iterator
from pydantic import PositiveInt
import stamina

from src.config.models.inputs.google import GoogleConfig, StorageInputs, GoogleInputs
from src.utils.time_conversion import from_milliseconds

logger = structlog.get_logger(__name__)


class ClientType(str, Enum):
    STORAGE = "storage"


class GoogleClientError(Exception):
    """Custom exception for GCP Client errors."""

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class GoogleClient:

    def __init__(self, config: GoogleConfig, limit: Optional[PositiveInt] = None):
        """Google Cloud client supporting Logs, Metrics, and Storage APIs."""
        self.config = config
        self.input_type = self.config.config_type
        self.storage_config = (
            self.config.storage_config if self.config.storage_config else None
        )
        self.logs_config = self.config.logs_config if self.config.logs_config else None
        self.storage_client = self._get_client(ClientType.STORAGE)
        self.call_map = {
            GoogleInputs.OBJECTS: self.storage_client.list_blobs,
        }
        self.storage_input = (
            self.storage_config.input_type if self.storage_config else None
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

    def _get_params(self, call_type: GoogleInputs) -> Dict[str, Any]:
        params = {}
        if call_type == GoogleInputs.OBJECTS and self.storage_config:
            params["bucket_name"] = self.storage_config.bucket_name
            if self.storage_config.prefix:
                params["prefix"] = self.storage_config.prefix
        return params

    @stamina.retry(on=GoogleClientError)
    def make_api_call(self, call_type: GoogleInputs, params: Dict[str, Any]) -> Any:
        if call_type not in self.call_map:
            raise ValueError(f"Unsupported call type: {call_type}")

        api_call = self.call_map[call_type]

        try:
            return api_call(**params)
        except Exception as e:
            msg = f"Unexpected error querying GCP: {str(e)}"
            logger.error(msg, exc_info=True)
            raise GoogleClientError(msg) from e

    def query_objects(
        self, start_time: PositiveInt, end_time: PositiveInt
    ) -> Iterator[Dict[str, Any]]:
        total_rows = 0
        bucket_name = self.storage_config.bucket_name if self.storage_config else None
        params = self._get_params(GoogleInputs.OBJECTS)
        if not params:
            raise ValueError("Missing required parameters for querying objects")

        blobs = self.make_api_call(self.input_type, params)
        start_date = from_milliseconds(start_time)
        end_date = from_milliseconds(end_time)

        for blob in blobs:

            # Check if the blob was created within the specified time range
            if start_date <= blob.time_created <= end_date:
                if self.storage_input == StorageInputs.LOGS:
                    try:
                        logger.debug(f"Processing blob: {blob.name}")
                        yield from self._process_blob_content_for_logs(blob)
                        total_rows += 1
                    except Exception as e:
                        msg = f"Error processing blob {blob.name}: {str(e)}"
                        logger.error(msg, exc_info=True)
                        raise GoogleClientError(msg) from e
                else:
                    # Extend this to support metrics in the future.
                    msg = "Unsupported storage config type"
                    logger.error(msg)
                    raise GoogleClientError(msg)

        logger.info(
            f"Processed {total_rows} blobs from GCP Storage",
            start_time=start_time,
            end_time=end_time,
            bucket_name=bucket_name,
        )

    def _process_blob_content_for_logs(
        self, blob: storage.Blob
    ) -> Iterator[Dict[str, Any]]:
        if not self.storage_config or not self.storage_config.logs_config:
            raise ValueError("Missing required logs config")

        log_features = self.storage_config.logs_config.log_features

        row_count = 0
        stream = blob.download_as_text().splitlines()
        for line_number, line in enumerate(stream, 1):
            # Split the line into individual JSON objects
            try:
                content = json.loads(line.strip())
                try:
                    log_id = content[log_features.id]
                    timestamp = content[log_features.timestamp]
                except KeyError as e:
                    error_msg = (
                        f"Missing required fields in {blob.name}, line {line_number}, object {row_count}. "
                        f"Error: {str(e)}. "
                        f"Problematic content: {json.dumps(content)[:200]}..."
                    )
                    logger.error(error_msg)
                    raise GoogleClientError(error_msg) from e

                row = {
                    "content": content,
                    "id": log_id,
                    "timestamp": timestamp,
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
                    f"Problematic content: {line[:200]}..."
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
