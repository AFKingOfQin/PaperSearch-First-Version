# AI-generated verification (Codex): partition Cornell v287 and independently reconcile its eligible revisions.
import argparse
import hashlib
import json
import sqlite3
from collections import Counter
from pathlib import Path
from zipfile import ZipFile

from papersearch.adapters import json_record, parse_record
from papersearch.archive import write_atomic
from papersearch.contracts import RecordError
from papersearch.snapshot_format import file_sha

ZIP_SHA = "5e4bee80eb200b20e857f14c575ad031f1501bdd65231189f14c1070c2de50a9"
MEMBER = "arxiv-metadata-oai-snapshot.json"
STAMP = "2026-11-21T00:00:00Z"  # Fixed import provenance; source revision time comes from the record.


def save(path, value):
    write_atomic(path, json.dumps(value, sort_keys=True).encode())


def oracle(db):
    query = """WITH valid AS (
        SELECT paper,version,stamp,min(event) AS event,min(hash) AS hash FROM revisions
        GROUP BY paper,version,stamp HAVING count(*)=1
    ), ranked AS (
        SELECT *,row_number() OVER(PARTITION BY paper ORDER BY version DESC,stamp DESC) AS n FROM valid
    ) SELECT paper,event,hash FROM ranked WHERE n=1 ORDER BY paper"""
    h = hashlib.sha256()
    count = 0
    for values in db.execute(query):
        h.update(("\t".join(values) + "\n").encode())
        count += 1
    return dict(
        current=count,
        identity_sha256=h.hexdigest(),
        distinct_valid_events=db.execute("SELECT COUNT(*) FROM revisions").fetchone()[
            0
        ],
    )


def prepare(source, target):
    if file_sha(source) != ZIP_SHA:
        raise ValueError("Cornell v287 ZIP checksum mismatch")
    target.mkdir(parents=True, exist_ok=False)
    blocks = target / "blocks"
    blocks.mkdir()
    db = sqlite3.connect(target / "expected.sqlite")
    db.execute("CREATE TABLE ids(id TEXT PRIMARY KEY) WITHOUT ROWID")
    db.execute(
        "CREATE TABLE revisions(paper TEXT,version INTEGER,stamp TEXT,event TEXT,hash TEXT, PRIMARY KEY(paper,version,stamp,event)) WITHOUT ROWID"
    )
    ids = number = chunk = total_bytes = 0
    data = bytearray()
    rejected = Counter()
    parts = []
    gates = {}

    def flush():
        nonlocal chunk
        chunk += 1
        name = f"{chunk:05d}.jsonl"
        write_atomic(blocks / name, data)
        info = dict(
            name=name,
            bytes=len(data),
            sha256=hashlib.sha256(data).hexdigest(),
            through=number,
        )
        parts.append(info)
        db.commit()
        save(blocks / f"{chunk:05d}.json", info)
        data.clear()

    with ZipFile(source) as z, z.open(MEMBER) as stream:
        for number, raw in enumerate(stream, 1):
            total_bytes += len(raw)
            data.extend(raw)
            try:
                e = parse_record(
                    json_record(raw),
                    "arxiv",
                    batch_id="oracle",
                    collected_at=STAMP,
                    location=f"line-{number}",
                    checksum=hashlib.sha256(raw).hexdigest(),
                )
                r = e.canonical()
            except RecordError as error:
                rejected[str(error)] += 1
            else:
                ids += db.execute(
                    "INSERT OR IGNORE INTO ids VALUES(?)", (e.paper_id,)
                ).rowcount
                db.execute(
                    "INSERT OR IGNORE INTO revisions VALUES(?,?,?,?,?)",
                    (
                        e.paper_id,
                        e.source_version or 0,
                        e.source_updated_at,
                        r["event_id"],
                        r["embedding_text_hash"],
                    ),
                )
            at_gate = ids == 1000000 and "million" not in gates
            if number % 4096 == 0 or at_gate:
                flush()
            if at_gate:
                gates["million"] = dict(
                    **oracle(db), lines=number, blocks=chunk, rejected=dict(rejected)
                )
                save(target / "million.json", gates["million"])
        if data:
            flush()
    gates["full"] = dict(
        **oracle(db), lines=number, blocks=chunk, rejected=dict(rejected)
    )
    db.close()
    save(
        target / "source.json",
        dict(
            url="https://api.kaggle.com/v1/datasets/download/Cornell-University/arxiv?datasetVersionNumber=287",
            zip_sha256=ZIP_SHA,
            zip_bytes=source.stat().st_size,
            member=MEMBER,
            uncompressed_bytes=total_bytes,
            parts=parts,
            gates=gates,
        ),
    )
    print(gates, flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("zip", type=Path)
    p.add_argument("output", type=Path)
    a = p.parse_args()
    prepare(a.zip, a.output)
