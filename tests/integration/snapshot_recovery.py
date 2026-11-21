# AI-generated tests (Codex): real worker exits, stale work, consistent Parquet and atomic publication.
import hashlib
import json
import os
import resource
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from contextlib import closing
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import pyarrow
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text

from papersearch import derivation, snapshots
from papersearch.config import mysql_url
from papersearch.derivation import TEST_KIND, cache_key, fixture_vector, shard_path
from papersearch.snapshot_format import VECTORS, file_sha, parquet_rows, validate
from papersearch.storage.metadata import apply_file
from papersearch.storage.reviews import create_project, read_decision, save_decision

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "phase1"
STAMP = "2020-02-01T00:00:00Z"
WORK = Path(sys.argv[1])
ROOT = WORK / "store"
DB = create_engine(mysql_url(dict(os.environ)))
CASES = (FIX / "cases.jsonl").read_bytes().splitlines(keepends=True)
EXPECTED = json.loads((FIX / "expected.json").read_bytes())["development"]


def check(name, actual, expected):
    assert actual == expected, f"{name}: expected {expected!r}, got {actual!r}"
    print(f"PASS {name}: {actual!r}", flush=True)


def run(name, args, *, crash=None, code=0):
    cmd = [str(v) for v in args]
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=180,
        env={
            **os.environ,
            "PAPERSEARCH_TEST_FAULTS": "1" if crash else "0",
            "PAPERSEARCH_TEST_CRASH": crash or "",
        },
    )
    (WORK / (name + ".log")).write_text(result.stdout + result.stderr)
    print(
        "RUN",
        " ".join(cmd),
        f"exit={result.returncode}",
        f"fault={crash}" if crash else "",
    )
    if result.returncode != code:
        raise AssertionError((result.stdout + result.stderr)[-2000:])
    return result.stdout.strip()


def snap(name, *args, root=ROOT, crash=None, code=0):
    output = run(
        name, ["papersearch", "snapshot", "--root", root, *args], crash=crash, code=code
    )
    return output if crash else json.loads(output)


def ingest(name, raw):
    path = WORK / (name + ".jsonl")
    path.write_bytes(raw)
    return apply_file(DB, path, "fixture", name, STAMP)


def scalar(sql):
    with DB.connect() as conn:
        return conn.scalar(text(sql))


def term(directory, word):
    with closing(
        sqlite3.connect(
            (directory / "lexical.sqlite").resolve().as_uri() + "?mode=ro&immutable=1",
            uri=True,
        )
    ) as db:
        return db.execute(
            "SELECT paper_id FROM documents WHERE documents MATCH ? ORDER BY paper_id",
            (word,),
        ).fetchall()


def verify_boundaries():
    ingest("seed", CASES[0])
    project = create_project(DB, "Streams", "count streams")
    seed = scalar("SELECT event_id FROM current_revisions")
    save_decision(
        DB,
        project,
        "fixture:river",
        seed,
        "relevant",
        "two streams reported",
        str(uuid4()),
    )
    snap("configure", "configure-fixture")
    first = snap("initial", "build")
    old_dir = ROOT / "snapshots" / first["snapshot_id"]
    check("published seed is searchable", term(old_dir, "two"), [("fixture:river",)])
    snap(
        "interrupted-artifact",
        "work",
        "--workers",
        "1",
        "--batch-size",
        "1",
        "--max-jobs",
        "1",
        "--lease-seconds",
        "2",
        crash="artifact-renamed",
        code=86,
    )
    check(
        "abandoned durable lease",
        scalar(
            f"SELECT COUNT(*) FROM derived_jobs WHERE kind='{TEST_KIND}' AND status='leased'"
        ),
        1,
    )
    check(
        "shard exists before SQL records it",
        [
            len(list((ROOT / "shards").glob("*/*"))),
            scalar("SELECT COUNT(*) FROM derived_artifacts"),
        ],
        [1, 0],
    )
    ingest("authors", CASES[3])
    check(
        "author correction reuses work identity",
        scalar(f"SELECT COUNT(*) FROM derived_jobs WHERE kind='{TEST_KIND}'"),
        1,
    )
    ingest("abstract", CASES[4])
    check(
        "abstract creates a new identity",
        scalar(f"SELECT COUNT(*) FROM derived_jobs WHERE kind='{TEST_KIND}'"),
        2,
    )
    expiry = time.monotonic() + 5
    while scalar(
        "SELECT COUNT(*) FROM derived_jobs WHERE status='leased' AND lease_until>UTC_TIMESTAMP(6)"
    ):
        if time.monotonic() > expiry:
            raise AssertionError("abandoned lease did not expire")
        time.sleep(0.1)
    reclaimed = snap("reclaim", "work", "--workers", "1", "--max-jobs", "1")
    check(
        "old-text completion reuses the renamed shard",
        [reclaimed["completed"], reclaimed["shards_reused"]],
        [1, 1],
    )
    check(
        "reader retains pinned old lexical package",
        term(old_dir, "two"),
        [("fixture:river",)],
    )
    check(
        "freshness shows unpublished metadata",
        snap("behind", "status")["metadata_generations_behind"],
        2,
    )
    for point in ("snapshot-write", "snapshot-renamed"):
        snap(point, "build", crash=point, code=86)
        check(
            point + " leaves previous snapshot valid",
            snap("verify-" + point, "verify")["snapshot_id"],
            first["snapshot_id"],
        )
    partial = snap("partial", "build")
    check(
        "stale vector excluded, revised paper searchable",
        partial["coverage"],
        dict(metadata=1, lexical=1, semantic=0, fixture_eligible=1, test_vectors=0),
    )
    check(
        "new package lexical text",
        term(ROOT / "snapshots" / partial["snapshot_id"], "three"),
        [("fixture:river",)],
    )
    store = derivation.store_shard

    def finish_after_reclaim(root, generated, **kwargs):
        stored = store(root, generated, **kwargs)
        # The old worker has bytes on disk but has not entered the SQL completion transaction.
        time.sleep(1.1)
        reclaimed = snap("new-work", "work", "--max-jobs", "1")
        check("new text cache miss", reclaimed["computed"], 1)
        return stored

    epoch = scalar("SELECT artifact_epoch FROM derivation_state")
    with patch.object(derivation, "store_shard", side_effect=finish_after_reclaim):
        late = derivation.process_batch(DB, ROOT, 1, 1)
    check(
        "late owner fenced after replacement completed",
        [
            late["lost_leases"],
            late["completed"],
            scalar("SELECT artifact_epoch FROM derivation_state") - epoch,
        ],
        [1, 0, 1],
    )
    check("replayed input", ingest("replay", CASES[4]), {"duplicate": 1})
    check("no work on unchanged replay", snap("unused", "work")["claimed"], 0)
    twin = {**json.loads(CASES[4]), "id": "sample-0000", "source_version": None}
    ingest("shared-text", json.dumps(twin).encode())
    cross = snap("hit", "work")
    check(
        "second paper reuses the same verified artifact",
        [cross["hits"], cross["computed"]],
        [1, 0],
    )
    for name, args in (
        ("model", ["--model-id", "fixture-other"]),
        ("revision", ["--model-id", "fixture-other", "--model-revision", "v2"]),
        (
            "preprocessing",
            [
                "--model-id",
                "fixture-other",
                "--model-revision",
                "v2",
                "--preprocessing",
                "text-v2",
            ],
        ),
    ):
        snap("switch-" + name, "configure-fixture", *args)
        before = scalar("SELECT COUNT(*) FROM derived_artifacts")
        results = snap("work-" + name, "work", "--workers", "1")
        check(
            name + " produces one new cache entry for two papers",
            [
                results["computed"],
                results["hits"],
                scalar("SELECT COUNT(*) FROM derived_artifacts") - before,
            ],
            [1, 1, 1],
        )

    snap("restore-fixture", "configure-fixture")
    after_restore = snap("restore-hit", "work")
    check("previous recipe works without recomputation", after_restore["claimed"], 0)
    # Exercise a genuine filesystem error without touching existing artifacts.
    ingest("additional", CASES[5])
    run(
        "wrong-store",
        ["papersearch", "snapshot", "--root", WORK / "wrong-store", "build"],
        code=1,
    )
    check(
        "misconfigured publisher leaves active pointer intact",
        snap("store-check", "verify")["snapshot_id"],
        partial["snapshot_id"],
    )
    with DB.connect() as conn:
        queued = (
            conn.execute(
                text(
                    "SELECT * FROM derived_jobs WHERE status='pending' AND kind=:kind ORDER BY job_id LIMIT 1"
                ),
                dict(kind=TEST_KIND),
            )
            .mappings()
            .one()
        )
    key = cache_key(queued["text_hash"], queued)
    blocked_path = shard_path(ROOT, hashlib.sha256(fixture_vector(key)).hexdigest())
    blocked_path.mkdir(parents=True)
    for i in range(3):
        result = snap("write-failure-" + str(i), "work", "--max-jobs", "1", code=1)
        check("filesystem error is recorded", result["errors"], 1)
    error = snap("visible-failure", "status")["recent_errors"][0]
    check(
        "terminal error visible and bounded",
        [error["status"], error["attempts"], bool(error["last_error"])],
        ["failed", 3, True],
    )
    blocked_path.rmdir()
    snap("retry", "retry", error["job_id"])
    check(
        "explicit retry succeeds after storage is restored",
        snap("retry-work", "work")["completed"],
        1,
    )

    # A separate DB connection commits a new paper between keyset pages of the build.
    first_page = snapshots.current_page

    def concurrent_import(*args):
        rows = first_page(*args)
        if args[1] == "":
            ingest("during-build", CASES[11])
        return rows

    with patch.object(snapshots, "current_page", side_effect=concurrent_import):
        view = snapshots.publish_snapshot(DB, ROOT, page_size=1)
    check(
        "consistent export excludes mid-build commit", view["coverage"]["metadata"], 3
    )
    check(
        "mid-build commit visible in freshness",
        snap("consistent-view", "status")["metadata_generations_behind"],
        1,
    )
    # Allow another independent publisher to win between validation and pointer switch.
    real_validate = snapshots.validate

    def publish_competitor(directory, sha):
        validated = real_validate(directory, sha)
        with patch.object(snapshots, "validate", real_validate):
            snap("winning-builder", "build")
        return validated

    try:
        with patch.object(snapshots, "validate", side_effect=publish_competitor):
            snapshots.publish_snapshot(DB, ROOT)
    except ValueError as exc:
        assert "Concurrent publication" in str(exc)
    else:
        raise AssertionError("A losing builder replaced the winning pointer")
    winner = json.loads((WORK / "winning-builder.log").read_text())
    check(
        "late builder cannot replace winner",
        snap("verify-winner", "verify")["snapshot_id"],
        winner["snapshot_id"],
    )
    return project


def footprints():
    with DB.connect() as conn:
        conn.execute(text("SET SESSION information_schema_stats_expiry=0"))
        sql_bytes = conn.scalar(
            text(
                "SELECT SUM(data_length+index_length) FROM information_schema.tables "
                "WHERE table_schema=DATABASE()"
            )
        )
    return dict(
        work_bytes=sum(p.stat().st_size for p in WORK.rglob("*") if p.is_file()),
        sql_bytes=int(sql_bytes),
    )


def client_cpu():
    usage = [
        resource.getrusage(who)
        for who in (resource.RUSAGE_SELF, resource.RUSAGE_CHILDREN)
    ]
    return sum(r.ru_utime + r.ru_stime for r in usage)


def full_fixture(project):
    topic = "papersearch.m4"
    initial_size = footprints()
    start_cpu = client_cpu()
    started = time.monotonic()
    run("generate", [sys.executable, FIX / "generate.py", WORK / "development.jsonl"])
    run("init", ["papersearch", "stream", "init", topic])
    batch = run(
        "archive",
        [
            "papersearch",
            "stream",
            "archive",
            WORK / "development.jsonl",
            "--archive-root",
            WORK / "archives",
            "--format",
            "fixture",
            "--collected-at",
            STAMP,
            "--batch-id",
            "development",
        ],
    )
    check(
        "fixture SHA-256",
        json.loads((Path(batch) / "manifest.json").read_bytes())["sha256"],
        EXPECTED["input_sha256"],
    )
    run("send", ["papersearch", "stream", "publish", topic, batch])
    drained = json.loads(
        run(
            "consume", ["papersearch", "stream", "consume", topic, "--drain"]
        ).splitlines()[-1]
    )
    check(
        "all Kafka partitions drained",
        [p["lag"] for p in drained["partitions"].values()],
        [0, 0, 0],
    )
    check("all input deliveries accounted for", sum(drained["outcomes"].values()), 1000)
    before = snap("ingestion-done", "status")
    assert any(
        j["kind"] == TEST_KIND and j["status"] == "pending" for j in before["jobs"]
    )
    work = snap(
        "full-workers",
        "work",
        "--workers",
        "2",
        "--batch-size",
        "32",
        "--max-jobs",
        "2048",
    )
    print("Worker results", json.dumps(work, sort_keys=True))
    check("every claimed job completed", work["completed"], work["claimed"])
    manifest = snap("full-build", "build")
    check("active package verified", snap("full-verify", "verify"), manifest)
    check(
        "M1 corpus digest from reopened Parquet rows",
        manifest["corpus_sha256"],
        EXPECTED["state_sha256"],
    )
    check(
        "full package coverage",
        manifest["coverage"],
        dict(
            metadata=986,
            lexical=986,
            semantic=0,
            fixture_eligible=986,
            test_vectors=986,
        ),
    )
    decision = read_decision(DB, project, "fixture:river")
    check(
        "review survives archive and snapshot",
        [decision["label"], decision["note"], decision["needs_reconsideration"]],
        ["relevant", "two streams reported", True],
    )
    active = ROOT / "snapshots" / manifest["snapshot_id"]
    check("lexical fixture lookup", term(active, "streams"), [("fixture:river",)])
    sample = next(parquet_rows(active / "test-vectors.parquet", VECTORS))
    with DB.connect() as conn:
        entry = dict(
            conn.execute(
                text("SELECT * FROM derived_artifacts WHERE cache_key=:key"),
                dict(key=sample["cache_key"]),
            )
            .mappings()
            .one()
        )
    print("Sample cache record", json.dumps(entry, sort_keys=True))
    print("Final status", json.dumps(snap("final-status", "status"), sort_keys=True))
    elapsed = time.monotonic() - started
    print(
        "Fixture observation",
        json.dumps(
            dict(
                seconds=round(elapsed, 3),
                client_cpu_seconds=round(client_cpu() - start_cpu, 3),
                input_lines_per_second=round(1000 / elapsed, 2),
                client_cgroup_peak_bytes=int(
                    Path("/sys/fs/cgroup/memory.peak").read_text()
                ),
                before=initial_size,
                after=footprints(),
                pyarrow=pyarrow.__version__,
                sqlite=sqlite3.sqlite_version,
            )
        ),
    )
    # Re-check alignment even if a malformed package supplies matching file checksums.
    with tempfile.TemporaryDirectory(dir=WORK) as tmp:
        copy = Path(tmp) / "package"
        shutil.copytree(active, copy)
        vector_file = copy / "test-vectors.parquet"
        altered = list(parquet_rows(vector_file, VECTORS))
        altered[0]["paper_id"] = "fixture:wrong"
        pq.write_table(pyarrow.Table.from_pylist(altered, schema=VECTORS), vector_file)
        bad = json.loads((copy / "manifest.json").read_bytes())
        bad["files"][vector_file.name] = dict(
            bytes=vector_file.stat().st_size, sha256=file_sha(vector_file)
        )
        (copy / "manifest.json").write_text(json.dumps(bad))
        try:
            validate(copy, file_sha(copy / "manifest.json"))
        except ValueError as exc:
            check(
                "mismatched vector mapping rejected despite matching file hashes",
                str(exc),
                "Invalid test-vector identity or row mapping",
            )
        else:
            raise AssertionError("Invalid row mapping accepted")


if __name__ == "__main__":
    WORK.mkdir(parents=True, exist_ok=False)
    full_fixture(verify_boundaries())
