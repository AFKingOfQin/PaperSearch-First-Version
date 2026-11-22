# AI-generated integration check (Codex): run the real offline Spark job against the original contract fixture.
import json
import os
import subprocess
from pathlib import Path
from tempfile import mkdtemp

import pyarrow as pa
import pyarrow.parquet as pq

from papersearch.adapters import raw_lines, json_record, parse_record
from papersearch.batch.reference_merge import HISTORY, sorted_rows
from papersearch.contracts import RecordError, digest
from papersearch.snapshot_format import PAYLOAD, flat_row


def main():
    repo = Path(__file__).resolve().parents[1]
    scratch = repo / "artifacts"
    scratch.mkdir(exist_ok=True)
    folder = Path(mkdtemp(prefix="spark-fixture-", dir=scratch))
    fixture = repo / "tests/fixtures/phase1"
    parts = {"base": [], "updates": []}
    rejects = 0
    for number, checksum, line in raw_lines(fixture / "cases.jsonl"):
        try:
            event = parse_record(
                json_record(line),
                "fixture",
                batch_id="fixture",
                collected_at="2020-02-01T00:00:00Z",
                location=f"cases#{number}",
                checksum=checksum,
            )
        except RecordError:
            rejects += 1
            continue
        side = "base" if number in {1, 3, 6, 9, 10, 12} else "updates"
        parts[side].append(flat_row(0, event.canonical()))
    for name, rows in parts.items():
        pq.write_table(
            pa.Table.from_pylist(rows[::-1], schema=HISTORY), folder / f"{name}.parquet"
        )
    subprocess.run(
        [
            "docker",
            "compose",
            "--profile",
            "batch",
            "run",
            "--rm",
            "--pull",
            "never",
            "--no-deps",
            "spark",
            "--master",
            "local[2]",
            "--driver-memory",
            "1g",
            "--conf",
            "spark.sql.shuffle.partitions=4",
            "--conf",
            "spark.ui.enabled=false",
            "/jobs/spark_merge.py",
            "/batch/base.parquet",
            "/batch/updates.parquet",
            "/batch/result",
        ],
        cwd=repo,
        env={**os.environ, "BATCH_DIR": str(folder)},
        check=True,
    )
    papers = {}
    changes = {}
    for row in sorted_rows(folder / "result/merged"):
        changes[row["paper_id"]] = row.pop("change")
        del row["previous_event_id"]
        if row["event_id"] is not None:
            row["payload"] = {k: row.pop(k) for k in PAYLOAD}
            papers[row["paper_id"]] = row
    expected = json.loads((fixture / "expected.json").read_bytes())["cases"]
    assert (
        digest(papers) == expected["state_sha256"] and rejects == expected["rejected"]
    )
    assert changes == {
        "fixture:bare": "new",
        "fixture:cafe": "unchanged",
        "fixture:river": "changed",
        "fixture:dispute": "changed",
        "fixture:withheld": "withdrawn",
    }, changes
    print(
        "PASS Spark fixture",
        digest(papers),
        changes,
        "normalized input rejects",
        rejects,
    )


if __name__ == "__main__":
    main()
