# AI-generated code (Codex): select nested reproducible subsets from a bounded Cornell ZIP prefix.
import argparse
import hashlib
import json
import struct
import zlib
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from papersearch.adapters import json_record, snapshot_record
from papersearch.contracts import RecordError

URL = "https://api.kaggle.com/v1/datasets/download/Cornell-University/arxiv?datasetVersionNumber=287"
GATES = (10000, 50000, 100000)


def lines(source):
    header = source.read(30)
    signature, _, flags, method, _, _, _, _, _, name_len, extra_len = struct.unpack(
        "<4s5H3I2H", header
    )
    name = source.read(name_len)
    if (signature, method, name) != (
        b"PK\x03\x04",
        8,
        b"arxiv-metadata-oai-snapshot.json",
    ) or flags & 1:
        raise ValueError("Unexpected first ZIP member")
    source.read(extra_len)
    decoder = zlib.decompressobj(-15)
    tail = b""
    while block := source.read(65536):
        rows = (tail + decoder.decompress(block)).split(b"\n")
        tail = rows.pop()
        for row in rows:
            yield row + b"\n"
        if decoder.eof:
            break


def select(prefix, output):

    seen, skipped, batches, gates = set(), Counter(), [], []
    cumulative = hashlib.sha256()
    collected = datetime.fromtimestamp(prefix.stat().st_mtime, timezone.utc).isoformat()
    with prefix.open("rb") as source:
        prefix_sha = hashlib.file_digest(source, "sha256").hexdigest()
        if (
            prefix_sha
            != "f3e42c9069f4d8befd0e60129019a5a7700c51a321bf095e84ffb2ee6305ab5f"
        ):
            raise ValueError(
                "Expected the first 134217728 bytes of the pinned v287 ZIP"
            )
        output.mkdir(parents=True, exist_ok=False)
        source.seek(0)
        rows = enumerate(lines(source), 1)
        for batch in range(50):
            path = output / f"batch-{batch:02}.jsonl"
            with path.open("xb") as out:
                for number, raw in rows:
                    try:
                        paper, *_ = snapshot_record(json_record(raw))
                    except RecordError as exc:
                        skipped[str(exc)] += 1
                        continue
                    if paper in seen:
                        skipped["duplicate_id"] += 1
                        continue
                    seen.add(paper)
                    out.write(raw)
                    cumulative.update(raw)
                    if len(seen) % 2000 == 0:
                        break
                else:
                    raise ValueError("ZIP prefix ended before target")
            with path.open("rb") as f:
                batches.append(
                    dict(
                        file=path.name,
                        bytes=path.stat().st_size,
                        sha256=hashlib.file_digest(f, "sha256").hexdigest(),
                    )
                )
            if len(seen) in GATES:
                gates.append(
                    dict(
                        distinct=len(seen),
                        source_lines=number,
                        skipped=dict(skipped),
                        sha256=cumulative.hexdigest(),
                        bytes=sum(b["bytes"] for b in batches),
                    )
                )
    result = dict(
        source=URL,
        prefix_bytes=prefix.stat().st_size,
        prefix_sha256=prefix_sha,
        collected_at=collected,
        selection="First accepted distinct IDs in source order, all categories and dates",
        gates=gates,
        batches=batches,
    )
    (output / "selection.json").write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("prefix", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    select(args.prefix, args.output)
