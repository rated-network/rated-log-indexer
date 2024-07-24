from pydantic import StrictStr, BaseModel


class CloudwatchConfig(BaseModel):
    region_name: StrictStr
    aws_access_key_id: StrictStr
    aws_secret_access_key: StrictStr
    log_group: StrictStr
