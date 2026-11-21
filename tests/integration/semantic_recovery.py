# AI-generated integration check (Codex): real CPU encoding, process restart, cache hits and stale-text exclusion.
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text

from papersearch.encoder import RECIPE, eligible
from papersearch.derivation import configure_recipe
from papersearch.config import mysql_url
from papersearch.storage.metadata import apply_file
from papersearch.snapshots import publish_snapshot, active_snapshot
from papersearch.semantic_search import SemanticReader
from papersearch.lexical import LexicalReader
from papersearch.snapshot_format import SEMANTIC, file_sha, validate


def main(root):
    engine = create_engine(mysql_url(os.environ))
    root.mkdir(parents=True)
    names = [f"semantic-{i}" for i in range(500) if eligible(f"fixture:semantic-{i}")][
        :3
    ]
    a, b, c = names

    def add(name, version, title, abstract):
        path = root / "input.jsonl"
        path.write_text(
            json.dumps(
                dict(
                    id=name,
                    source_version=version,
                    source_updated_at=f"2020-01-0{version}T00:00:00Z",
                    title=title,
                    abstract=abstract,
                )
            )
            + "\n"
        )
        assert apply_file(engine, path, "fixture", "m6", "2020-02-01T00:00:00Z") == {
            "applied": 1
        }

    def worker(*args, env=None, code=0):
        cmd = [
            "papersearch",
            "snapshot",
            "--root",
            str(root),
            "embed",
            "--batch-size",
            "1",
            *args,
        ]
        result = subprocess.run(
            cmd, capture_output=True, text=True, env={**os.environ, **(env or {})}
        )
        assert result.returncode == code, (cmd, result.stdout, result.stderr)
        return json.loads(result.stdout) if code == 0 else result.stdout + result.stderr

    configure_recipe(engine, RECIPE)
    add(a, 1, "River survey", "")
    add(b, 1, "Seismic waves", "Tracking earthquakes.")
    assert "semantic-batch-claimed" in worker(
        "--lease-seconds",
        "2",
        env={
            "PAPERSEARCH_TEST_FAULTS": "1",
            "PAPERSEARCH_TEST_CRASH": "semantic-batch-claimed",
        },
        code=86,
    )
    with engine.connect() as conn:
        assert (
            conn.scalar(text("SELECT COUNT(*) FROM derived_jobs WHERE status='leased'"))
            == 1
        )
    time.sleep(2.1)
    assert "semantic-batch-committed" in worker(
        env={
            "PAPERSEARCH_TEST_FAULTS": "1",
            "PAPERSEARCH_TEST_CRASH": "semantic-batch-committed",
        },
        code=86,
    )
    partial = publish_snapshot(engine, root)
    assert (
        partial["coverage"]["semantic"]
        == partial["coverage"]["semantic_title_only"]
        == 1
    )
    print(
        "PASS worker terminated after first durable batch; partial published coverage 1/2"
    )
    add(c, 1, "River survey", "")
    worker(env={"PAPERSEARCH_MODEL_DIR": str(root / "missing-model")}, code=1)
    result = worker()
    assert (result["claimed"], result["computed"], result["hits"]) == (2, 1, 1), result
    assert worker()["claimed"] == 0
    print("PASS restart and cache reuse", result)
    full = publish_snapshot(engine, root)
    lexical = LexicalReader(root)
    reader = SemanticReader(lexical)
    hits, _, _ = reader.search(full, "freshwater river study", 0, 3)
    assert len(hits) == 3
    scores = {r["paper_id"]: r["score"] for r in hits}
    assert scores["fixture:" + a] == scores["fixture:" + c]
    add(a, 2, "River survey", "New flood measurements.")
    revised = publish_snapshot(engine, root)
    assert revised["coverage"]["semantic"] == 2
    assert all(
        h["paper_id"] != "fixture:" + a
        for h in reader.search(revised, "river", 0, 3)[0]
    )
    assert worker()["computed"] == 1
    configure_recipe(engine, RECIPE)
    assert worker()["claimed"] == 0
    final = publish_snapshot(engine, root)
    assert active_snapshot(engine, root)[1] == final
    assert (
        final["coverage"]["semantic"] == 3
        and final["coverage"]["semantic_title_only"] == 1
    )
    with engine.connect() as conn:
        assert conn.scalar(text("SELECT COUNT(*) FROM derived_artifacts")) == 3
    print(
        "PASS old vector excluded after changed text; new revision published; title-only and stable equal-score retrieval",
        final["coverage"],
    )
    # Change the mapping and update the outer checksums. The cross-file identity check must still fail.
    copy = root / "invalid-package"
    shutil.copytree(root / "snapshots" / final["snapshot_id"], copy)
    path = copy / "semantic.parquet"
    rows = pq.read_table(path).to_pylist()
    rows[0]["event_id"] = rows[1]["event_id"]
    pq.write_table(pa.Table.from_pylist(rows, schema=SEMANTIC), path)
    final["files"][path.name] = dict(bytes=path.stat().st_size, sha256=file_sha(path))
    manifest = copy / "manifest.json"
    manifest.write_text(json.dumps(final))
    try:
        validate(copy, file_sha(manifest))
    except ValueError as exc:
        assert "semantic vector identity or row mapping" in str(exc), exc
    else:
        raise AssertionError("Published vector accepted a different paper revision")
    print(
        "PASS cross-file validation rejected a swapped revision with matching file checksums"
    )


if __name__ == "__main__":
    main(Path(sys.argv[1]))
