from datetime import datetime
from typing import Optional

import httpx
import stamina
from pydantic import BaseModel

from src.config.models.output import RatedOutputConfig


class SlaosClient:
    def __init__(self, config: RatedOutputConfig):
        self.config = config
        self.client = httpx.Client()

    @property
    def full_ingest_url(self) -> str:
        base_url = self.config.ingestion_url
        ingestion_id = self.config.ingestion_id
        ingestion_key = self.config.ingestion_key

        return f"{base_url}/{ingestion_id}/{ingestion_key}"

    def get_latest_ingest_timestamp(
        self, datastream_key: str, customer_id: Optional[str] = None
    ) -> datetime | None:
        url = f"{self.full_ingest_url}/indexed-slis"
        params = {
            "key": datastream_key,
            "limit": "1",
        }

        if customer_id is not None:
            params["customer_id"] = customer_id

        headers = {
            "Content-Type": "application/json",
        }
        for attempt in stamina.retry_context(on=httpx.HTTPStatusError):
            with attempt:
                response = self.client.get(url, params=params, headers=headers)
                response.raise_for_status()

        documents = [SLIIndexedDocument(**doc) for doc in response.json()]

        if documents:
            return documents[0].timestamp
        else:
            return None


class SLIIndexedDocument(BaseModel):
    key: str
    timestamp: datetime
    report_timestamp: datetime
    ingestion_timestamp: datetime
