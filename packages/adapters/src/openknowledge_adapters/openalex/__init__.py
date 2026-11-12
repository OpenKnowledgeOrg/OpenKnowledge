"""OpenAlex snapshot on S3: a manifest of Parquet files, read over plain HTTPS without a key."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import NamedTuple

import httpx

from openknowledge_adapters.openalex.catalog import DELETED_PATH, MANIFEST_PATH, OpenAlexCatalog
from openknowledge_adapters.openalex.config import OpenAlexConfig
from openknowledge_adapters.semantic_scholar.downloads import (
    SignedLinkDownloads,
    install_log_redaction,
)


class OpenAlex(NamedTuple):
    catalog: OpenAlexCatalog
    downloads: SignedLinkDownloads


@asynccontextmanager
async def connect(
    config: OpenAlexConfig, *, transport: httpx.AsyncBaseTransport | None = None
) -> AsyncGenerator[OpenAlex, None]:
    """Open the manifest and file clients for one run; the transport is a test seam."""
    install_log_redaction()
    base = str(config.base_url)
    manifest = httpx.AsyncClient(
        base_url=base,
        timeout=config.timeout_seconds,
        follow_redirects=False,
        transport=transport,
    )
    files = httpx.AsyncClient(
        timeout=httpx.Timeout(
            config.download_read_timeout_seconds, connect=config.download_connect_timeout_seconds
        ),
        follow_redirects=False,
        transport=transport,
    )
    async with manifest, files:
        yield OpenAlex(
            catalog=OpenAlexCatalog(manifest, base), downloads=SignedLinkDownloads(files)
        )


__all__ = [
    "DELETED_PATH",
    "MANIFEST_PATH",
    "OpenAlex",
    "OpenAlexCatalog",
    "OpenAlexConfig",
    "connect",
]
