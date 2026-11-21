# AI-generated tests (Codex): archive integrity, acknowledgement failures and consumer safety guards.
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from papersearch.archive import archive, inspect_batch
from papersearch.broker import offsets
from papersearch.ingestion import consume
from papersearch.publication import publish

STAMP = "2020-02-01T00:00:00Z"


def test_archive_is_repeatable_and_tampering_blocks_publication(tmp_path):
    source = tmp_path / "source.jsonl"
    source.write_bytes(b"bad\r\nlast")
    batch = archive(source, tmp_path / "batches", "fixture", "batch", STAMP)
    assert archive(source, tmp_path / "batches", "fixture", "batch", STAMP) == batch
    assert inspect_batch(batch)["records"] == 2
    (batch / "records.jsonl").write_bytes(b"changed\n")
    with pytest.raises(ValueError, match="checksum"):
        publish("unused", "papers", batch)


def test_only_a_contiguous_acknowledged_window_becomes_a_checkpoint(tmp_path):
    source = tmp_path / "batch.jsonl"
    source.write_bytes(b"bad\n" * 40)
    batch = archive(source, tmp_path / "archives", "fixture", "batch", STAMP)
    producer = MagicMock()
    futures = [MagicMock() for _ in range(40)]
    for n, future in enumerate(futures):
        future.get.return_value = SimpleNamespace(partition=0, offset=n)
    futures[35].get.side_effect = TimeoutError("no broker acknowledgement")
    producer.send.side_effect = futures
    with (
        patch("papersearch.publication.topic_info", return_value="cluster"),
        patch("papersearch.publication.group_reader"),
        patch("papersearch.publication.offsets", return_value={}),
        patch("papersearch.publication.KafkaProducer", return_value=producer),
        pytest.raises(TimeoutError),
    ):
        publish("broker", "topic", batch)
    (checkpoint,) = batch.glob("ack-*.json")
    assert json.loads(checkpoint.read_bytes())["line"] == 32


@pytest.mark.parametrize("committed", [None, 3, 12])
def test_retention_or_truncation_requires_explicit_replay(committed):
    reader = MagicMock()
    reader.committed.return_value = committed
    reader.beginning_offsets.side_effect = lambda parts: dict.fromkeys(parts, 5)
    reader.end_offsets.side_effect = lambda parts: dict.fromkeys(parts, 10)
    with pytest.raises(ValueError, match="retained range"):
        offsets(reader, "papers")


def test_fault_hooks_require_the_explicit_test_gate(monkeypatch, tmp_path):
    monkeypatch.delenv("PAPERSEARCH_TEST_FAULTS", raising=False)
    with pytest.raises(ValueError, match="explicit test mode"):
        consume(
            None, "unused", "papers", "guard", tmp_path, fault="before-commit", at="1:3"
        )
