"""Import all Cornell v287 blocks into the reviewer and publish its complete BM25 index."""
import argparse
import hashlib
import json
import os
import sqlite3
from contextlib import closing
from itertools import batched
from pathlib import Path

from sqlalchemy import bindparam, create_engine, text

from papersearch.adapters import json_record, parse_record, raw_lines
from papersearch.config import mysql_url
from papersearch.contracts import RecordError
from papersearch.snapshot_format import file_sha
from papersearch.snapshots import publish_snapshot
from papersearch.storage.metadata import apply_event, apply_fresh_events, reject_record

SOURCE_SHA = "c8d3cf5eb3d532efecaafde1304bb1d50b9a21cb6e3dfb630361e8fedeccb56d"
SEEN = text("SELECT event_id FROM processed_events WHERE event_id IN :ids").bindparams(
    bindparam("ids", expanding=True)
)


def import_block(engine, path):
    for batch in batched(raw_lines(path), 256):
        events = {}
        for line, sha, raw in batch:
            location = f"{path.name}#{line}"
            try:
                event = parse_record(json_record(raw), "arxiv", batch_id="cornell-v287-full",
                                     collected_at="2026-11-21T00:00:00Z", location=location, checksum=sha)
            except RecordError as exc:
                reject_record(engine, sha, "arxiv", str(exc), location)
                continue
            events[event.canonical()["event_id"]] = event
        if events:
            with engine.connect() as conn:
                seen = set(conn.scalars(SEEN, dict(ids=list(events))))
            fresh = [event for key, event in events.items() if key not in seen]
            if fresh and not apply_fresh_events(engine, fresh):
                for event in fresh:
                    apply_event(engine, event)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("--store", type=Path, default=Path("/artifacts/derived"))
    parser.add_argument("--work-dir", type=Path, help="Fast local staging directory for the FTS build")
    parser.add_argument("--publish-only", action="store_true", help="Publish a previously imported full corpus")
    parser.add_argument("--require-all-semantic", action="store_true", help="Only publish if every eligible paper has a vector")
    args = parser.parse_args()
    source = args.source / "source.json"
    if file_sha(source) != SOURCE_SHA:
        raise ValueError("Expected the pinned full Cornell v287 block manifest")
    manifest = json.loads(source.read_bytes())
    engine = create_engine(mysql_url(os.environ), pool_pre_ping=True)
    for n, part in enumerate([] if args.publish_only else manifest["parts"], 1):
        path = args.source / "blocks" / part["name"]
        if file_sha(path) != part["sha256"]:
            raise ValueError(f"Block checksum mismatch: {path}")
        import_block(engine, path)
        print(f"Imported or replayed block {n}/{len(manifest['parts'])}", flush=True)

    def verify(directory, package):
        coverage = package["coverage"]
        if args.require_all_semantic and (not coverage["semantic_eligible"] or coverage["semantic"] != coverage["semantic_eligible"]):
            raise ValueError(f"Incomplete semantic coverage: {coverage}")
        expected = manifest["gates"]["full"]
        if package["coverage"]["lexical"] != expected["current"]:
            raise ValueError("Incomplete v287 corpus")
        digest = hashlib.sha256()
        uri = (directory / "lexical.sqlite").resolve().as_uri() + "?mode=ro&immutable=1"
        with closing(sqlite3.connect(uri, uri=True)) as db:
            for row in db.execute("SELECT paper_id,event_id,text_hash FROM documents ORDER BY rowid"):
                digest.update(("\t".join(row) + "\n").encode())
        if digest.hexdigest() != expected["identity_sha256"]:
            raise ValueError("Published revisions differ from the verified v287 corpus")
        return dict(source="Cornell arXiv v287", scope="full", records=expected["current"],
                    identity_sha256=digest.hexdigest())

    package = publish_snapshot(engine, args.store, page_size=1024, verify_collection=verify, work_dir=args.work_dir)
    print(package["snapshot_id"], package["coverage"], flush=True)


if __name__ == "__main__":
    main()
