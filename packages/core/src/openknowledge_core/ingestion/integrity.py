"""Checks on every byte of a shard as it streams: digests, counts and the gzip framing."""

from __future__ import annotations

import base64
import hashlib
import zlib
from collections.abc import Iterator

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, ConfigDict, NonNegativeInt

from openknowledge_contracts.manifests import Md5Base64, Sha256Hex
from openknowledge_core.ingestion.errors import (
    GzipInvalid,
    IncompleteTransfer,
    ParquetInvalid,
    RowCountMismatch,
)

MAX_OUTPUT_STEP = 8 * 1024 * 1024
MAX_FOOTER = 16 * 1024 * 1024
MAGIC = b"PAR1"


class IntegrityReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    size: NonNegativeInt
    sha256: Sha256Hex
    md5: Md5Base64
    gzip_members: NonNegativeInt
    records: NonNegativeInt
    uncompressed_size: NonNegativeInt


class GzipCheck:
    """``close`` fails unless every member ended, when zlib checks its CRC-32 and length."""

    def __init__(self) -> None:
        self.members = 0
        self.uncompressed_size = 0
        self._newlines = 0
        self._ends_with_newline = True
        self._start_member()

    def _start_member(self) -> None:
        self._z = zlib.decompressobj(16 + zlib.MAX_WBITS)
        self._open = False

    @property
    def records(self) -> int:
        """Lines of the decompressed text, counting a last line that has no newline."""
        return self._newlines + (0 if self._ends_with_newline else 1)

    def update(self, data: bytes) -> None:
        """Feed the next bytes, which may span any number of members."""
        for _ in self.decode(data):
            pass

    def decode(self, data: bytes) -> Iterator[bytes]:
        """Yield bounded decompressed blocks while applying the same framing checks."""
        while data:
            if not self._open and self.members:
                data = data.lstrip(b"\x00")  # gzip tools accept zero padding after a member
                if not data:
                    return
            self._open = True
            try:
                out = self._z.decompress(data, MAX_OUTPUT_STEP)
            except zlib.error as exc:
                raise GzipInvalid(f"the gzip stream does not decode: {exc}") from None
            if out:
                self.uncompressed_size += len(out)
                self._newlines += out.count(b"\n")
                self._ends_with_newline = out.endswith(b"\n")
                yield out
            if self._z.eof:
                self.members += 1
                data = self._z.unused_data
                self._start_member()
            else:
                data = self._z.unconsumed_tail

    def close(self) -> None:
        if self.members == 0 or self._open:
            raise GzipInvalid("the gzip stream ends before its last member does")


class StreamCheck:
    def __init__(self) -> None:
        self._sha256 = hashlib.sha256()
        self._md5 = hashlib.md5(usedforsecurity=False)
        self._size = 0
        self._gzip = GzipCheck()
        self.report: IntegrityReport | None = None

    def update(self, data: bytes) -> None:
        for _ in self.decode(data):
            pass

    def decode(self, data: bytes) -> Iterator[bytes]:
        self._sha256.update(data)
        self._md5.update(data)
        self._size += len(data)
        yield from self._gzip.decode(data)

    def finish(self, expected_size: int) -> IntegrityReport:
        self._gzip.close()
        if self._size != expected_size:
            raise IncompleteTransfer(f"received {self._size} bytes where {expected_size} were due")
        self.report = IntegrityReport(
            size=self._size,
            sha256=self._sha256.hexdigest(),
            md5=base64.b64encode(self._md5.digest()).decode(),
            gzip_members=self._gzip.members,
            records=self._gzip.records,
            uncompressed_size=self._gzip.uncompressed_size,
        )
        return self.report


class ParquetCheck:
    """Digests a Parquet object verbatim and counts its rows from the footer alone.

    Only the first four bytes and a bounded tail are kept, however large the object is.
    """

    def __init__(self, expected_records: int | None = None) -> None:
        self._expected = expected_records
        self._sha256 = hashlib.sha256()
        self._md5 = hashlib.md5(usedforsecurity=False)
        self._size = 0
        self._head = b""
        self._tail = bytearray()
        self.report: IntegrityReport | None = None

    def update(self, data: bytes) -> None:
        if not data:
            return
        self._sha256.update(data)
        self._md5.update(data)
        self._size += len(data)
        if len(self._head) < len(MAGIC):
            self._head += data[: len(MAGIC) - len(self._head)]
        self._tail += data[-(MAX_FOOTER + 8) :]
        del self._tail[: -(MAX_FOOTER + 8)]

    def finish(self, expected_size: int) -> IntegrityReport:
        if self._size != expected_size:
            raise IncompleteTransfer(f"received {self._size} bytes where {expected_size} were due")
        rows = self._footer_rows()
        if self._expected is not None and rows != self._expected:
            raise RowCountMismatch(
                f"the Parquet footer counts {rows} rows, but the source states {self._expected}"
            )
        self.report = IntegrityReport(
            size=self._size,
            sha256=self._sha256.hexdigest(),
            md5=base64.b64encode(self._md5.digest()).decode(),
            gzip_members=0,
            records=rows,
            uncompressed_size=self._size,
        )
        return self.report

    def _footer_rows(self) -> int:
        tail = bytes(self._tail)
        if self._size < 12 or self._head != MAGIC or tail[-4:] != MAGIC:
            raise ParquetInvalid("the object lacks the PAR1 magic at its start or end")
        length = int.from_bytes(tail[-8:-4], "little")
        if length > MAX_FOOTER:
            raise ParquetInvalid(f"the Parquet footer claims {length} bytes, over {MAX_FOOTER}")
        if length + 12 > self._size:
            raise ParquetInvalid(f"the Parquet footer claims {length} bytes, more than the object")
        footer = tail[-8 - length : -8]
        try:
            reader = pa.BufferReader(MAGIC + footer + tail[-8:-4] + MAGIC)
            metadata = pq.read_metadata(reader)  # pyright: ignore[reportUnknownMemberType]
        except (pa.ArrowException, OSError) as exc:
            raise ParquetInvalid(f"the Parquet footer does not parse: {exc}") from None
        return metadata.num_rows


type ObjectCheck = StreamCheck | ParquetCheck
