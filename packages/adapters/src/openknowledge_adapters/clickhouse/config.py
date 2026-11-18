from __future__ import annotations

from pydantic import BaseModel, ConfigDict, HttpUrl, PositiveFloat, SecretStr

from openknowledge_contracts.manifests import BucketName


class ClickHouseConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    url: HttpUrl
    user: str
    password: SecretStr
    database: str = "openknowledge"
    raw_bucket: BucketName | None = None
    """The bucket the server's ``raw`` named collection reads; None for a reader."""
    timeout_seconds: PositiveFloat | None
