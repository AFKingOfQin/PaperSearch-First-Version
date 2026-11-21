# AI-generated code (Codex): reconcile the nested real-data gates through the production ingestion path.
import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import time
from contextlib import closing
from pathlib import Path

from sqlalchemy import create_engine, text

from papersearch.archive import archive, write_atomic
from papersearch.broker import group_reader, offsets, topic_info
from papersearch.config import mysql_url
from papersearch.ingestion import consume
from papersearch.publication import publish
from papersearch.snapshots import publish_snapshot
from papersearch.snapshot_format import file_sha

parser = argparse.ArgumentParser()
parser.add_argument(
    "--target", type=int, choices=(10000, 50000, 100000), default=100000
)
target = parser.parse_args().target
root = Path("/artifacts")
source = root / "selection"
manifest = json.loads((source / "selection.json").read_bytes())
broker, group, topic = "kafka:9092", "papersearch-ingest", "papers"
engine = create_engine(mysql_url(os.environ), pool_pre_ping=True)
topic_info(broker, topic, create=True)
# A verified earlier gate can be reused after stopping or restarting the verifier.
# Validate the database's publication identity before trusting a retained result.
completed = 0
for gate in manifest["gates"]:
    size = gate["distinct"]
    report = root / f"gate-{size}.json"
    if not report.exists() or size > target:
        break
    prior = json.loads(report.read_bytes())["snapshot"]
    package = root / "derived/snapshots" / prior["snapshot_id"] / "manifest.json"
    with engine.connect() as conn:
        expected = conn.scalar(
            text("SELECT manifest_sha FROM published_snapshots WHERE snapshot_id=:id"),
            {"id": prior["snapshot_id"]},
        )
    if file_sha(package) != expected or json.loads(package.read_bytes()) != prior:
        raise RuntimeError(
            "Retained gate does not belong to this database and artifact store"
        )
    completed = size
started = time.monotonic()
for index, batch in enumerate(manifest["batches"], 1):
    size = index * 2000
    if size > target:
        break
    if size <= completed:
        continue
    path = source / batch["file"]
    with path.open("rb") as f:
        assert hashlib.file_digest(f, "sha256").hexdigest() == batch["sha256"]
    if shutil.disk_usage(root).free < 100 * 2**30:
        raise RuntimeError("Artifact reserve below 100 GiB")
    directory = archive(
        path, root / "archives", "arxiv", path.stem, manifest["collected_at"]
    )
    publish(broker, topic, directory, group=group)
    ack = time.time()
    consume(engine, broker, topic, group, root / "quarantine", drain=True)
    durable = time.time()
    if size not in {g["distinct"] for g in manifest["gates"]}:
        continue
    with engine.connect() as conn:
        counts = {
            table: conn.scalar(text("SELECT COUNT(*) FROM " + table))
            for table in (
                "current_revisions",
                "processed_events",
                "rejected_events",
                "derived_jobs",
            )
        }
        db_bytes = int(
            conn.scalar(
                text(
                    "SELECT SUM(data_length+index_length) FROM information_schema.tables WHERE table_schema=DATABASE()"
                )
            )
        )
    with closing(group_reader(broker, group)) as reader:
        partitions = offsets(reader, topic)
    assert list(counts.values()) == [size, size, 0, 0] and not any(
        p["lag"] for p in partitions.values()
    )
    snapshot = publish_snapshot(engine, root / "derived", page_size=128)
    assert snapshot["coverage"]["lexical"] == snapshot["coverage"]["metadata"] == size
    with closing(
        sqlite3.connect(
            root / "derived/snapshots" / snapshot["snapshot_id"] / "lexical.sqlite"
        )
    ) as db:
        t = time.monotonic()
        hits = db.execute(
            "SELECT paper_id FROM documents WHERE documents MATCH 'quantum' ORDER BY bm25(documents,3,1),rowid LIMIT 10"
        ).fetchall()
        query_ms = 1000 * (time.monotonic() - t)
        assert len(hits) == 10
    result = dict(
        snapshot=snapshot,
        counts=counts,
        partitions=partitions,
        mysql_allocated_bytes=db_bytes,
        last_batch_ack_time=ack,
        last_batch_durable_time=durable,
        visible_time=time.time(),
        elapsed_seconds=time.monotonic() - started,
        query_ms=query_ms,
        first_hit=hits[0][0],
        container_peak_bytes=int(Path("/sys/fs/cgroup/memory.peak").read_text()),
        artifact_free_bytes=shutil.disk_usage(root).free,
    )
    write_atomic(root / f"gate-{size}.json", json.dumps(result, indent=2).encode())
    print("VERIFIED GATE", size, flush=True)
