# AI-generated tests (Codex): field weighting, safe FTS terms, immutable revisions and stable pagination.
import sqlite3
from contextlib import closing

import pytest

from papersearch.lexical import LexicalReader
from papersearch.snapshot_format import file_sha


def test_bm25_weights_filters_pagination_and_cold_checksum(tmp_path):
    directory = tmp_path / "snapshots" / "snapshot"
    directory.mkdir(parents=True)
    path = directory / "lexical.sqlite"
    with closing(sqlite3.connect(path)) as db:
        db.execute(
            "CREATE VIRTUAL TABLE documents USING fts5(title,abstract,paper_id UNINDEXED,event_id UNINDEXED,text_hash UNINDEXED)"
        )
        db.executemany(
            "INSERT INTO documents VALUES(?,?,?,?,?)",
            [
                ("Quantum", "A discussion", "a", "1", "x"),
                ("Methods", "A quantum discussion", "b", "2", "y"),
                ("Methods", "A quantum discussion", "c", "3", "z"),
            ],
        )
        db.commit()
    manifest = dict(
        snapshot_id="snapshot",
        files={
            "lexical.sqlite": dict(bytes=path.stat().st_size, sha256=file_sha(path))
        },
    )
    reader = LexicalReader(tmp_path)
    first, more = reader.search(manifest, 'quantum"*', "all", 0, 1)
    assert more and first[0]["paper_id"] == "a"
    rest, more = reader.search(manifest, "quantum", "all", 1, 2)
    assert not more and [(r["rank"], r["paper_id"]) for r in rest] == [
        (2, "b"),
        (3, "c"),
    ]
    hits, more = reader.search(manifest, "quantum", "title", 0, 2)
    assert not more and [r["event_id"] for r in hits] == ["1"]
    hits, _ = reader.search(manifest, "quantum", "abstract", 0, 3)
    assert [r["paper_id"] for r in hits] == ["b", "c"]
    with pytest.raises(ValueError):
        reader.search(manifest, "***", "all", 0, 1)
    manifest["files"]["lexical.sqlite"]["sha256"] = "0" * 64
    with pytest.raises(OSError, match="checksum"):
        reader.search(manifest, "quantum", "all", 0, 1)
