# AI-generated code (Codex): create an isolated base and update history from a verified published corpus.
import argparse
import json
import hashlib
from collections import Counter
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from papersearch.adapters import parse_record
from papersearch.batch.reference_merge import HISTORY
from papersearch.contracts import Event, Payload
from papersearch.snapshot_format import PAYLOAD, file_sha, flat_row, validate


def alternate(row, title, revision, stamp):
    payload = Payload.parse({**{k: row[k] for k in PAYLOAD}, "title": title})
    event = Event(
        row["source"], row["paper_id"], revision, stamp, "", "", "", "", payload
    )
    return flat_row(0, event.canonical())


def prepare(snapshot, manifest, output):
    expected = json.loads(manifest.read_bytes())
    actual = validate(snapshot, file_sha(snapshot / "manifest.json"))
    if actual != expected or actual["coverage"]["metadata"] != 100000:
        raise ValueError("Expected the verified 100k publication")
    output.mkdir(parents=True, exist_ok=False)
    for side in ("base", "updates"):
        (output / side).mkdir()
    counts = Counter()
    inputs = {}

    def save(rows, side, part):
        if not rows:
            return
        path = output / side / f"part-{part}.parquet"
        pq.write_table(
            pa.Table.from_pylist(rows, schema=HISTORY), path, compression="snappy"
        )
        inputs[path.relative_to(output).as_posix()] = dict(
            rows=len(rows), bytes=path.stat().st_size, sha256=file_sha(path)
        )

    with pq.ParquetFile(snapshot / "corpus.parquet") as papers:
        for part, batch in enumerate(
            papers.iter_batches(batch_size=5000, use_threads=False)
        ):
            base, updates = [], []
            for row in batch.to_pylist():
                number = row["row_id"]
                if number > 90000:
                    updates.append(row)
                    counts["real_new"] += 1
                    continue
                case = number % 100
                if case in (0, 2):
                    stale = alternate(
                        row,
                        "Synthetic earlier title",
                        None,
                        "1900-01-01T00:00:00.000000Z",
                    )
                    if case == 0:
                        base.append(stale)
                        # Exercise Spark's set/sort normalization on a winning row, keeping hashes canonical.
                        updates.append(
                            dict(
                                row,
                                categories=list(reversed(row["categories"]))
                                + row["categories"][:1],
                            )
                        )
                        counts["updated_from_synthetic_baseline"] += 1
                        continue
                    updates.append(stale)
                    counts["stale_update"] += 1
                base.append(row)
                if case == 1:
                    updates.append(row)
                    counts["identical_replay"] += 1
                if case in (3, 4):
                    v = (row["source_version"] or 0) + 1
                    t = "2090-01-01T00:00:00.000000Z"
                    first = alternate(row, "Synthetic conflicting title A", v, t)
                    (base if case == 4 else updates).append(first)
                    updates.append(
                        alternate(row, "Synthetic conflicting title B", v, t)
                    )
                    counts[
                        "retract_to_prior" if case == 4 else "conflicting_updates"
                    ] += 1
            save(base, "base", part)
            save(updates, "updates", part)
    # A disputed sole priority exercises loss of a current record without changing the real final corpus.
    for side, title in (("base", "First"), ("updates", "Second")):
        row = dict(
            id="batch-withdrawn", title=title, source_updated_at="2000-01-01T00:00:00Z"
        )
        raw = json.dumps(row).encode()
        event = parse_record(
            row,
            "fixture",
            batch_id="batch-control",
            collected_at="2020-01-01T00:00:00Z",
            location="synthetic-control",
            checksum=hashlib.sha256(raw).hexdigest(),
        )
        save([flat_row(0, event.canonical())], side, "control")
    report = dict(
        snapshot_id=actual["snapshot_id"],
        corpus_sha256=actual["corpus_sha256"],
        source_parquet=actual["files"]["corpus.parquet"],
        cases=dict(counts),
        parts=inputs,
        synthetic_withdrawals=1,
        seed=None,
        split="sorted corpus rows 1..90000 base, 90001..100000 new; base cases by row_id modulo 100",
    )
    (output / "inputs.json").write_text(json.dumps(report, indent=2))
    print(f"Prepared {len(inputs)} partition files", dict(counts))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    for name in ("snapshot", "manifest", "output"):
        p.add_argument(name, type=Path)
    args = p.parse_args()
    prepare(args.snapshot, args.manifest, args.output)
