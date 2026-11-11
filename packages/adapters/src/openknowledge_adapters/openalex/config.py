from __future__ import annotations

from pydantic import BaseModel, ConfigDict, HttpUrl, PositiveFloat


class OpenAlexConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    base_url: HttpUrl = HttpUrl("https://openalex.s3.amazonaws.com/")
    timeout_seconds: PositiveFloat
    download_connect_timeout_seconds: PositiveFloat
    download_read_timeout_seconds: PositiveFloat
