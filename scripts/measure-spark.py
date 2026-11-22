# AI-generated benchmark (Codex): time equivalent local reference and Spark merges on immutable inputs.
import argparse
import json
import os
import platform
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

import psutil

from papersearch.batch.reference_merge import write_reference
from papersearch.snapshot_format import file_sha

REPO = Path(__file__).resolve().parents[1]
SOURCES = [
    "compose.yaml",
    "requirements.lock",
    "src/papersearch/contracts.py",
    "src/papersearch/adapters.py",
    "src/papersearch/snapshot_format.py",
    "src/papersearch/reference.py",
    "src/papersearch/batch/reference_merge.py",
    "src/papersearch/batch/spark_merge.py",
    "scripts/prepare-spark-input.py",
    "scripts/measure-spark.py",
]


def measure(folder, run):
    inputs = json.loads((folder / "inputs.json").read_bytes())
    for relative, details in inputs["parts"].items():
        if file_sha(folder / relative) != details["sha256"]:
            raise ValueError(f"Input checksum mismatch: {relative}")
    name = f"trial-{run}"
    settings = [
        "--master",
        "local[2]",
        "--driver-memory",
        "1g",
        "--conf",
        "spark.sql.shuffle.partitions=4",
        "--conf",
        "spark.sql.adaptive.enabled=false",
        "--conf",
        "spark.ui.enabled=false",
        "--conf",
        "spark.driver.host=127.0.0.1",
        "--conf",
        "spark.sql.files.maxPartitionBytes=32m",
    ]
    cmd = [
        "docker",
        "compose",
        "--profile",
        "batch",
        "run",
        "--rm",
        "--no-deps",
        "--pull",
        "never",
        "spark",
        *settings,
        "/jobs/spark_merge.py",
        "/batch/base",
        "/batch/updates",
        f"/batch/{name}",
        "--measure",
    ]
    if run % 2 == 0:
        cmd.append("--parquet-first")
    report = dict(
        trial=run,
        utc=datetime.now(timezone.utc).isoformat(),
        driver_arguments=cmd,
        base_commit=subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=REPO, text=True
        ).strip(),
        dirty=bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=REPO)),
        sources={p: file_sha(REPO / p) for p in SOURCES},
        host=platform.platform(),
        host_python=platform.python_version(),
        logical_cpus=psutil.cpu_count(),
        physical_cpus=psutil.cpu_count(logical=False),
        ram_bytes=psutil.virtual_memory().total,
        storage_order="parquet,csv,csv,parquet"
        if run % 2 == 0
        else "csv,parquet,parquet,csv",
    )
    if (folder / name).exists() or (folder / f"reference-{run}").exists():
        raise FileExistsError(
            "Choose an unused trial number or fresh benchmark directory"
        )
    start = time.perf_counter()
    write_reference(folder / "base", folder / "updates", folder / f"reference-{run}")
    report["reference_merge_seconds"] = time.perf_counter() - start
    report["reference_process_peak_bytes"] = psutil.Process().memory_info().peak_wset
    with (folder / f"{name}.log").open("x") as log:
        start = time.perf_counter()
        subprocess.run(
            [*cmd, "--launched-at", str(time.time())],
            cwd=REPO,
            env={**os.environ, "BATCH_DIR": str(folder)},
            stdout=log,
            stderr=subprocess.STDOUT,
            check=True,
        )
        report["whole_container_seconds"] = time.perf_counter() - start
    report.update(json.loads((folder / name / "report.json").read_bytes()))
    (folder / f"{name}.json").write_text(json.dumps(report, indent=2))
    print(
        name,
        {key: value for key, value in report.items() if key.endswith("_seconds")},
        flush=True,
    )


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("folder", type=Path)
    p.add_argument("trials", nargs="+", type=int)
    args = p.parse_args()
    for trial in args.trials:
        measure(args.folder.resolve(), trial)
