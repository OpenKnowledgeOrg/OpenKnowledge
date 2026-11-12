import io
import json
import struct

import pytest

from openknowledge_core.embeddings import ROW_BYTES, VECTOR_DIM, IndexRow, convert_shard


def record(corpusid: int, vector: list[object]) -> bytes:
    return json.dumps(
        {"corpusid": corpusid, "model": "specter_v2", "vector": json.dumps(vector)}
    ).encode()


def test_base_shard_writes_vectors_and_index() -> None:
    second = [0.1 * (i % 7) for i in range(VECTOR_DIM)]
    blob = io.BytesIO()

    rows = convert_shard([record(9, [0.5] * VECTOR_DIM), record(3, [*second])], "base", blob)

    assert len(blob.getvalue()) == 2 * ROW_BYTES == 6144
    row_1 = struct.unpack(f"<{VECTOR_DIM}f", blob.getvalue()[ROW_BYTES:])
    assert row_1 == struct.unpack(f"<{VECTOR_DIM}f", struct.pack(f"<{VECTOR_DIM}f", *second))
    assert rows == [IndexRow(3, 1, False), IndexRow(9, 0, False)]


def test_repeated_corpusid_keeps_last_row() -> None:
    lines = [record(5, [1.0] * VECTOR_DIM), record(7, [2.0] * VECTOR_DIM)]
    lines.append(record(5, [3.0] * VECTOR_DIM))
    blob = io.BytesIO()

    rows = convert_shard(lines, "updates", blob)

    assert rows == [IndexRow(5, 2, False), IndexRow(7, 1, False)]
    assert struct.unpack_from("<f", blob.getvalue(), 2 * ROW_BYTES) == (3.0,)


def test_deletes_write_only_tombstones() -> None:
    blob = io.BytesIO()
    lines = [b'{"corpusid": 8}', b'{"corpusid": 2}', b'{"corpusid": 8}']

    rows = convert_shard(lines, "deletes", blob)

    assert blob.getvalue() == b""
    assert rows == [IndexRow(2, 0, True), IndexRow(8, 0, True)]


@pytest.mark.parametrize(
    "vector",
    [[0.0] * (VECTOR_DIM - 1), [1e39, *[0.0] * 767], ["x", *[0.0] * 767]],
    ids=["767 values", "1e39", "not a number"],
)
def test_bad_vector_names_the_paper(vector: list[object]) -> None:
    with pytest.raises(ValueError, match="corpusid 123456789"):
        convert_shard([record(123456789, vector)], "base", io.BytesIO())
