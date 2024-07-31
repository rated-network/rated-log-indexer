from pydantic import BaseModel, StrictStr


class OutputYamlConfig(BaseModel):
    slaos_api_key: StrictStr
    ingestion_id: StrictStr
    ingestion_key: StrictStr
