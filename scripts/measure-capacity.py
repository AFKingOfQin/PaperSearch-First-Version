# AI-generated orchestration (Codex): disk-guarded, restartable capacity runs with independent host measurements.
import argparse
import json
import os
import shutil
import subprocess
import time
from pathlib import Path

import psutil

from papersearch.config import settings, disk_preflight
from papersearch.snapshot_format import file_sha

REPO = Path(__file__).resolve().parents[1]


def run(folder, pilot_only):
    project = "papersearch-m8-capacity"
    env = {
        **os.environ,
        "ARTIFACT_DIR": str(folder),
        "MYSQL_MEMORY": "4g",
        "CORE_CPUS": "2",
    }
    config = {**settings(REPO / ".env"), **env}
    if not disk_preflight(config, True):
        raise RuntimeError("Scale reserve unavailable")
    network = folder / "capacity-network.yaml"
    network.write_text(
        "services:\n  mysql:\n    command: [\"--innodb-buffer-pool-size=2G\", \"--innodb-redo-log-capacity=1G\"]\n"
        "networks:\n  default:\n    ipam:\n      config:\n        - subnet: 10.238.4.0/24\n"
    )
    compose = [
        "docker",
        "compose",
        "-f",
        str(REPO / "compose.yaml"),
        "-f",
        str(network),
        "-p",
        project,
        "--profile",
        "core",
        "--profile",
        "snapshot",
    ]

    def call(*args, **kwargs):
        return subprocess.run(compose + list(args), env=env, check=True, **kwargs)

    folder.mkdir(exist_ok=True, parents=True)
    call("build", "snapshot")
    call("up", "-d", "--wait", "mysql", "kafka")
    try:
        call(
            "run",
            "--rm",
            "--no-deps",
            "--entrypoint",
            "alembic",
            "snapshot",
            "upgrade",
            "head",
        )
        runner = [
            "run",
            "--rm",
            "--no-deps",
            "--name",
            project + "-import",
            "-v",
            f"{REPO / 'scripts'}:/verify:ro",
            "--entrypoint",
            "python",
            "snapshot",
            "-u",
            "/verify/run-capacity.py",
        ]
        recorded = folder / "capacity-source.json"
        sources = [
            *sorted((REPO / "src").rglob("*.py")),
            *sorted((REPO / "migrations").rglob("*.py")),
            *(
                REPO / p
                for p in [
                    "compose.yaml",
                    "Dockerfile",
                    "requirements.lock",
                    "alembic.ini",
                    "scripts/run-capacity.py",
                    "scripts/prepare-capacity.py",
                    "scripts/measure-capacity.py",
                ]
            ),
        ]
        source = dict(
            base_commit=subprocess.check_output(
                ["git", "rev-parse", "HEAD"], text=True
            ).strip(),
            dirty=bool(subprocess.check_output(["git", "status", "--porcelain"])),
            sha256={p.relative_to(REPO).as_posix(): file_sha(p) for p in sources},
        )
        with (folder / "capacity-attempts.jsonl").open("a") as meta:
            meta.write(json.dumps(dict(start=time.time(), **source)) + "\n")
        if not recorded.exists():
            recorded.write_text(json.dumps(source, indent=2))
        if not (folder / "capacity" / "state.json").exists():
            proc = subprocess.run(compose + runner + ["--stop-after", "1"], env=env)
            if proc.returncode != 86:
                raise RuntimeError(
                    f"Expected capacity interruption, got {proc.returncode}"
                )
        if pilot_only:
            return
        with (
            (folder / "capacity-runtime.log").open("a") as log,
            (folder / "resources.jsonl").open("a") as stats,
        ):
            worker = subprocess.Popen(
                compose + runner, env=env, stdout=log, stderr=subprocess.STDOUT
            )
            while worker.poll() is None:
                disks = {
                    k: shutil.disk_usage(v).free
                    for k, v in {
                        "artifact": folder,
                        "docker_host": config["DOCKER_DISK_DIR"],
                    }.items()
                }
                sample = dict(
                    time=time.time(),
                    free_bytes=disks,
                    host_available_ram=psutil.virtual_memory().available,
                )
                for service in ("mysql", "kafka"):
                    sample[service] = int(
                        call(
                            "exec",
                            "-T",
                            service,
                            "cat",
                            "/sys/fs/cgroup/memory.peak",
                            capture_output=True,
                        ).stdout
                    )
                stats.write(json.dumps(sample) + "\n")
                stats.flush()
                if min(disks.values()) < 100 * 2**30:
                    subprocess.run(["docker", "stop", project + "-import"], check=True)
                    raise RuntimeError("100 GiB disk reserve crossed")
                try:
                    worker.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    pass
            if worker.returncode:
                raise RuntimeError(
                    f"Capacity import exited {worker.returncode}; inspect {log.name}"
                )
    finally:
        call("stop", "mysql", "kafka")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("folder", type=Path)
    p.add_argument("--pilot-only", action="store_true")
    a = p.parse_args()
    run(a.folder.resolve(), a.pilot_only)
