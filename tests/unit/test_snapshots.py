# AI-generated tests (Codex): Parquet schema, corpus identity and immutable fixture cache validation.
import hashlib

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from papersearch.contracts import digest
from papersearch.derivation import (
    TEST_KIND,
    cache_key,
    fixture_vector,
    shard_path,
    verified_vector,
)
from papersearch.snapshot_format import CORPUS, VECTORS, CorpusDigest, parquet_rows


def test_streamed_manifest_digest_matches_reference():
    rows = {
        "fixture:a": {"paper_id": "fixture:a", "title": "Montréal"},
        "fixture:b": {"paper_id": "fixture:b", "categories": [], "published": None},
    }
    streamed = CorpusDigest()
    assert streamed.result() == digest({})
    for row in rows.values():
        streamed.add(row)
    assert streamed.result() == digest(rows)


def test_empty_parquet_keeps_schema(tmp_path):
    path = tmp_path / "empty.parquet"
    pq.write_table(pa.Table.from_pylist([], schema=CORPUS), path)
    assert list(parquet_rows(path, CORPUS)) == []
    with pytest.raises(ValueError, match="schema"):
        list(parquet_rows(path, VECTORS))


def test_shard_verifies_bytes_slot_and_encoder_identity(tmp_path):
    recipe = dict(
        kind=TEST_KIND, model_id="fixture", model_revision="r1", preprocessing="text-v1"
    )
    key = cache_key("1" * 64, recipe)
    vector = fixture_vector(key)
    packed = bytes(32) + vector
    shard = hashlib.sha256(packed).hexdigest()
    path = shard_path(tmp_path, shard)
    path.parent.mkdir(parents=True)
    path.write_bytes(packed)
    entry = dict(
        shard_sha=shard,
        slot=1,
        vector_sha=hashlib.sha256(vector).hexdigest(),
        cache_key=key,
    )
    assert verified_vector(tmp_path, entry, {}) == vector
    for invalid in (
        {"slot": 0},
        {"slot": 2},
        {"cache_key": "0" * 64},
        {"vector_sha": "0" * 64},
    ):
        with pytest.raises(ValueError, match="vector"):
            verified_vector(tmp_path, {**entry, **invalid}, {})
    path.write_bytes(packed[:-1])
    with pytest.raises(ValueError, match="shard"):
        verified_vector(tmp_path, entry, {})
