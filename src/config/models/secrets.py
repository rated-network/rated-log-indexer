from typing import Optional

from pydantic import BaseModel, StrictBool, StrictStr


class SecretsYamlConfig(BaseModel):
    use_secrets_manager: Optional[StrictBool] = False
    provider: Optional[StrictStr] = None
