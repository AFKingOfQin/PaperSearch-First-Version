# AI-generated code (Codex): consistent search package builds and atomic MySQL publication.
import json
import os
import sqlite3
import tempfile
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import text

from .archive import sync_dir, write_atomic
from .derivation import FIELDS, TEST_KIND, bind_store, crash_at, verified_vector
from .snapshot_format import (
    CORPUS,
    FILES,
    VECTORS,
    CorpusDigest,
    file_sha,
    flat_row,
    validate,
)
from .storage.metadata import current_page

STATE = """SELECT d.*,s.generation FROM derivation_state d JOIN system_state s
           ON d.singleton=s.singleton WHERE d.singleton=1"""


def write_package(engine, root, directory, snapshot_id, page_size):
    corpus = CorpusDigest()
    indexed = eligible = 0
    with (
        engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn,
        conn.begin(),
        closing(sqlite3.connect(directory / "lexical.sqlite")) as db,
        pq.ParquetWriter(
            directory / "corpus.parquet", CORPUS, compression="zstd"
        ) as papers,
        pq.ParquetWriter(
            directory / "test-vectors.parquet", VECTORS, compression="zstd"
        ) as vectors,
    ):
        state = conn.execute(text(STATE)).mappings().one()
        recipe = {field: state[field] for field in FIELDS}
        db.execute(
            "CREATE VIRTUAL TABLE documents USING fts5(title,abstract,paper_id UNINDEXED,"
            "event_id UNINDEXED,text_hash UNINDEXED,tokenize='unicode61')"
        )
        after = ""
        while rows := current_page(conn, after, page_size, recipe):
            metadata, mapping, buffers = [], [], {}
            for item in rows:
                record = json.loads(item.record_json)
                corpus.add(record)
                metadata.append(flat_row(corpus.count, record))
                after = item.paper_id
                eligible += after.startswith("fixture:")
                if item.cache_key and recipe["kind"] == TEST_KIND:
                    mapping.append(
                        dict(
                            vector_row=indexed,
                            row_id=corpus.count,
                            paper_id=after,
                            event_id=record["event_id"],
                            text_hash=record["embedding_text_hash"],
                            cache_key=item.cache_key,
                            value=verified_vector(root, item._mapping, buffers),
                        )
                    )
                    indexed += 1
            papers.write_table(pa.Table.from_pylist(metadata, schema=CORPUS))
            db.executemany(
                "INSERT INTO documents(rowid,title,abstract,paper_id,event_id,text_hash) "
                "VALUES(:row_id,:title,:abstract,:paper_id,:event_id,:embedding_text_hash)",
                metadata,
            )
            if mapping:
                vectors.write_table(pa.Table.from_pylist(mapping, schema=VECTORS))
            crash_at("snapshot-write")
        db.execute("INSERT INTO documents(documents) VALUES('integrity-check')")
        db.commit()
    files = {}
    for name in FILES:
        path = directory / name
        with path.open("rb+") as f:
            os.fsync(f.fileno())
        files[name] = dict(bytes=path.stat().st_size, sha256=file_sha(path))
    manifest = dict(
        format_version=1,
        snapshot_id=snapshot_id,
        created_at=datetime.now(timezone.utc).isoformat(),
        generation=state["generation"],
        artifact_epoch=state["artifact_epoch"],
        recipe=recipe,
        corpus_sha256=corpus.result(),
        eligibility="paper_id starts with fixture:",
        lexical_format="SQLite FTS5 unicode61",
        test_vector_format="SHA-256 fixture bytes, width 32",
        coverage=dict(
            metadata=corpus.count,
            lexical=corpus.count,
            semantic=0,
            fixture_eligible=eligible,
            test_vectors=indexed,
        ),
        files=files,
    )
    write_atomic(
        directory / "manifest.json",
        json.dumps(manifest, sort_keys=True, indent=2).encode() + b"\n",
    )
    return state


def publish_snapshot(engine, root: Path, page_size=32):
    if not 1 <= page_size <= 128:
        raise ValueError("page_size must be between 1 and 128")
    bind_store(engine, root)
    snapshots = root / "snapshots"
    snapshots.mkdir(parents=True, exist_ok=True)
    snapshot_id = uuid4().hex
    with tempfile.TemporaryDirectory(prefix=".building-", dir=snapshots) as staging:
        directory = Path(staging) / "package"
        directory.mkdir()
        view = write_package(engine, root, directory, snapshot_id, page_size)
        sha = file_sha(directory / "manifest.json")
        manifest = validate(directory, sha)
        directory.rename(snapshots / snapshot_id)
        sync_dir(snapshots)
    crash_at("snapshot-renamed")
    with engine.begin() as conn:
        live = conn.execute(
            text(
                "SELECT active_snapshot,recipe_epoch FROM derivation_state "
                "WHERE singleton=1 FOR UPDATE"
            )
        ).one()
        if (
            live.active_snapshot != view["active_snapshot"]
            or live.recipe_epoch != view["recipe_epoch"]
        ):
            raise ValueError(
                "Concurrent publication or recipe change; rebuild against the new state"
            )
        conn.execute(
            text("INSERT INTO published_snapshots VALUES(:id,:sha,:gen,:epoch)"),
            dict(
                id=snapshot_id,
                sha=sha,
                gen=view["generation"],
                epoch=view["artifact_epoch"],
            ),
        )
        conn.execute(
            text("UPDATE derivation_state SET active_snapshot=:id WHERE singleton=1"),
            dict(id=snapshot_id),
        )
    return manifest


def active_snapshot(engine, root):
    with engine.connect() as conn:
        row = conn.execute(
            text("""SELECT p.snapshot_id,p.manifest_sha FROM derivation_state s
            JOIN published_snapshots p ON p.snapshot_id=s.active_snapshot WHERE s.singleton=1""")
        ).first()
    if row is None:
        raise ValueError("No published snapshot")
    directory = root / "snapshots" / row.snapshot_id
    manifest = validate(directory, row.manifest_sha)
    if manifest["snapshot_id"] != row.snapshot_id:
        raise ValueError("Published snapshot identity mismatch")
    return directory, manifest


def published_manifest(conn, root, snapshot_id):
    if snapshot_id is None:
        return None
    expected = conn.scalar(
        text("SELECT manifest_sha FROM published_snapshots WHERE snapshot_id=:id"),
        dict(id=snapshot_id),
    )
    path = root / "snapshots" / snapshot_id / "manifest.json"
    if file_sha(path) != expected:
        raise ValueError("Active manifest checksum mismatch")
    manifest = json.loads(path.read_bytes())
    if manifest["snapshot_id"] != snapshot_id:
        raise ValueError("Published snapshot identity mismatch")
    return manifest


def status(engine, root):
    with (
        engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn,
        conn.begin(),
    ):
        state = conn.execute(text(STATE)).mappings().one()
        jobs = (
            conn.execute(
                text(
                    "SELECT kind,status,COUNT(*) AS n FROM derived_jobs GROUP BY kind,status "
                    "ORDER BY kind,status"
                )
            )
            .mappings()
            .all()
        )
        errors = (
            conn.execute(
                text(
                    "SELECT job_id,status,attempts,last_error FROM derived_jobs WHERE last_error "
                    "IS NOT NULL ORDER BY job_id DESC LIMIT 10"
                )
            )
            .mappings()
            .all()
        )
        active = published_manifest(conn, root, state["active_snapshot"])
        return dict(
            corpus_generation=state["generation"],
            artifact_epoch=state["artifact_epoch"],
            recipe={k: state[k] for k in FIELDS},
            jobs=[dict(j) for j in jobs],
            recent_errors=[dict(e) for e in errors],
            active=active,
            metadata_generations_behind=None
            if not active
            else state["generation"] - active["generation"],
            artifacts_changed_since_build=bool(
                active and active["artifact_epoch"] != state["artifact_epoch"]
            ),
            recipe_changed_since_build=bool(
                active and any(active["recipe"][k] != state[k] for k in FIELDS)
            ),
        )
