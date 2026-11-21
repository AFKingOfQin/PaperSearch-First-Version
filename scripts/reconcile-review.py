# AI-generated code (Codex): reconcile every selected source identity against the published result, offline.
import argparse
import hashlib
import json
import sqlite3
import tempfile
from contextlib import closing
from itertools import zip_longest
from pathlib import Path

from papersearch.adapters import json_record, parse_record, raw_lines
from papersearch.snapshot_format import file_sha, validate


def reconcile(root, through):
    source = root / "selection"
    subset = json.loads((source / "selection.json").read_bytes())
    raw = hashlib.sha256()
    gates = {g["distinct"]: g for g in subset["gates"]}
    count = 0
    with (
        tempfile.TemporaryDirectory(dir=root) as tmp,
        closing(sqlite3.connect(Path(tmp) / "expected.sqlite")) as expected,
    ):
        expected.execute("CREATE TABLE papers(id TEXT PRIMARY KEY,event TEXT NOT NULL)")
        for batch in subset["batches"]:
            path = source / batch["file"]
            if file_sha(path) != batch["sha256"]:
                raise ValueError(f"Input checksum mismatch: {path.name}")
            for number, checksum, line in raw_lines(path):
                raw.update(line)
                record = parse_record(
                    json_record(line),
                    "arxiv",
                    batch_id=path.stem,
                    collected_at=subset["collected_at"],
                    location=f"{path.name}#{number}",
                    checksum=checksum,
                ).canonical()
                expected.execute(
                    "INSERT INTO papers VALUES(?,?)",
                    (record["paper_id"], record["event_id"]),
                )
                count += 1
            if count not in gates:
                continue
            if raw.hexdigest() != gates[count]["sha256"]:
                raise ValueError(f"Nested input hash mismatch at {count}")
            measured = json.loads((root / f"gate-{count}.json").read_bytes())[
                "snapshot"
            ]
            dirpath = root / "derived/snapshots" / measured["snapshot_id"]
            live = validate(dirpath, file_sha(dirpath / "manifest.json"))
            assert live == measured and live["coverage"]["lexical"] == count
            uri = (
                dirpath / "lexical.sqlite"
            ).resolve().as_uri() + "?mode=ro&immutable=1"
            with closing(sqlite3.connect(uri, uri=True)) as index:
                rows = index.execute(
                    "SELECT paper_id,event_id FROM documents ORDER BY rowid"
                )
                ordered = expected.execute("SELECT id,event FROM papers ORDER BY id")
                if any(a != b for a, b in zip_longest(ordered, rows)):
                    raise ValueError(f"Source and index identities differ at {count}")
            print(
                f"PASS {count} raw-selected IDs and event IDs equal every ordered index row; "
                f"corpus SHA256 {live['corpus_sha256']}",
                flush=True,
            )

            if count == through:
                return
    raise ValueError(f"Missing selected gate {through}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument(
        "--through", type=int, choices=(10000, 50000, 100000), default=100000
    )
    args = parser.parse_args()
    reconcile(args.root, args.through)
