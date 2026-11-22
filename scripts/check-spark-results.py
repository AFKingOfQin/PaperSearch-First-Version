# AI-generated verification (Codex): compare every output record, storage cell and summary to the reference.
import argparse
import csv
import hashlib
import json
from collections import Counter
from itertools import zip_longest
from pathlib import Path

from papersearch.batch.reference_merge import sorted_rows
from papersearch.contracts import digest
from papersearch.snapshot_format import CorpusDigest, PAYLOAD, file_sha


def csv_rows(path):
    with path.open(encoding="utf-8", newline="") as source:
        yield from csv.DictReader(source)


def compare_trial(folder, trial, corpus_digest):
    name = f"trial-{trial}"
    root = folder / name
    report = json.loads((folder / f"{name}.json").read_bytes())
    frames = [
        sorted_rows(p)
        for p in (folder / f"reference-{trial}", root / "merged", root / "warm")
    ]
    csv_data = sorted_rows(root / "csv", csv_rows, "part-*.csv")
    parquet_data = sorted_rows(root / "parquet")
    corpus = CorpusDigest()
    changes = Counter()
    categories = Counter()
    output_hash = hashlib.sha256()
    data_hash = hashlib.sha256()
    last = ""
    for ref, first, warm in zip_longest(*frames):
        if ref is None or ref != first or ref != warm or ref["paper_id"] <= last:
            raise ValueError(
                f"Merge row/identity/classification disagreement after {last}"
            )
        last = ref["paper_id"]
        output_hash.update(bytes.fromhex(digest(ref)))
        changes[ref["change"]] += 1
        if ref["event_id"] is None:
            continue
        row = ref.copy()
        del row["change"], row["previous_event_id"]
        row["payload"] = {k: row.pop(k) for k in PAYLOAD}
        corpus.add(row)
        year = int((ref["published"] or "0000")[:4])
        if year == 2008:
            categories.update(ref["categories"])
        expected = [
            last,
            ref["event_id"],
            ref["embedding_text_hash"],
            year,
            ref["categories"],
            {"title": ref["title"], "abstract": ref["abstract"]},
        ]
        for source in (csv_data, parquet_data):
            item = next(source, None)
            if (
                item is None
                or [
                    item["paper_id"],
                    item["event_id"],
                    item["embedding_text_hash"],
                    int(item["year"]),
                    json.loads(item["categories"]),
                    json.loads(item["text"]),
                ]
                != expected
            ):
                raise ValueError(f"Storage cell mismatch for {last}")
        data_hash.update(bytes.fromhex(digest(expected)))
    if next(csv_data, None) is not None or next(parquet_data, None) is not None:
        raise ValueError("Unexpected extra storage rows")
    if changes != {"new": 10000, "changed": 1800, "unchanged": 88200, "withdrawn": 1}:
        raise ValueError(f"Unexpected update classifications: {changes}")
    if corpus.result() != corpus_digest or categories != report["categories_2008"]:
        raise ValueError("Source corpus digest or category summaries differ")
    outputs = {}
    for directory in (
        folder / f"reference-{trial}",
        root / "merged",
        root / "warm",
        root / "csv",
        root / "parquet",
    ):
        outputs[directory.relative_to(folder).as_posix()] = {
            p.name: dict(bytes=p.stat().st_size, sha256=file_sha(p))
            for p in sorted(directory.glob("part-*"))
        }
    report["checks"] = dict(
        current=corpus.count,
        corpus_sha256=corpus.result(),
        changes=dict(changes),
        merge_rows_sha256=output_hash.hexdigest(),
        storage_rows_sha256=data_hash.hexdigest(),
        category_query_sha256=digest(dict(categories)),
        category_groups=len(categories),
    )
    report["files"] = outputs
    print(name, report["checks"], flush=True)
    return report


def main():
    p = argparse.ArgumentParser()
    p.add_argument("folder", type=Path)
    p.add_argument("report", type=Path)
    p.add_argument("plans", type=Path)
    p.add_argument("trials", nargs="+", type=int)
    a = p.parse_args()
    inputs = json.loads((a.folder / "inputs.json").read_bytes())
    physical = {
        f.relative_to(a.folder).as_posix(): f
        for side in ("base", "updates")
        for f in (a.folder / side).glob("*.parquet")
    }
    if physical.keys() != inputs["parts"].keys() or any(
        file_sha(path) != inputs["parts"][name]["sha256"]
        for name, path in physical.items()
    ):
        raise ValueError("Input partition inventory differs from the prepared manifest")
    results = [compare_trial(a.folder, i, inputs["corpus_sha256"]) for i in a.trials]
    if any(trial["checks"] != results[0]["checks"] for trial in results):
        raise ValueError("Trial digests differ")
    plans = a.folder / f"trial-{a.trials[0]}"
    a.plans.write_text(
        "\n".join(
            f"=== {name} ===\n" + (plans / f"{name}-plan.txt").read_text()
            for name in ("merge", "csv", "parquet")
        ),
        encoding="utf-8",
    )
    a.report.write_text(
        json.dumps(dict(inputs=inputs, trials=results), indent=2) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
