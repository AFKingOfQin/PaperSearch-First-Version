# AI-generated experiment launcher (Codex): run each workload in a fresh schema and Kafka group.
import argparse
import json
import os
import platform
import subprocess
import time
from pathlib import Path
from uuid import uuid4

from papersearch.snapshot_format import file_sha

ROOT = Path(__file__).resolve().parents[1]
MODES = (
    "full incremental bypass derived2 workers1 workers2 workers3 before after".split()
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("folder", type=Path)
    parser.add_argument("models", type=Path)
    args = parser.parse_args()
    folder = args.folder.resolve()
    run = "m8x" + uuid4().hex[:8]
    output = folder / run
    output.mkdir()
    network = output / "network.yaml"
    network.write_text(
        "networks:\n  default:\n    ipam:\n      config:\n        - subnet: 10.238.5.0/24\n"
    )
    env = {
        **os.environ,
        "ARTIFACT_DIR": str(folder),
        "EMBED_MEMORY": "3g",
        "CORE_CPUS": "2",
    }
    compose = [
        "docker",
        "compose",
        "-f",
        str(ROOT / "compose.yaml"),
        "-f",
        str(network),
        "-p",
        "papersearch-m8-lab",
        "--profile",
        "core",
        "--profile",
        "embedding",
    ]

    def call(*args, **kw):
        return subprocess.run([*compose, *args], env=env, check=True, **kw)

    sources = [
        *ROOT.glob("requirements*.lock"),
        *ROOT.glob("Dockerfile*"),
        ROOT / "compose.yaml",
        ROOT / "alembic.ini",
        *sorted((ROOT / "src").rglob("*.py")),
        *sorted((ROOT / "migrations").rglob("*.py")),
        ROOT / "scripts/controlled-experiments.py",
        Path(__file__).resolve(),
    ]
    meta = dict(
        start=time.time(),
        run=run,
        platform=platform.platform(),
        base_commit=subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip(),
        dirty=bool(subprocess.check_output(["git", "status", "--porcelain"])),
        sha256={p.relative_to(ROOT).as_posix(): file_sha(p) for p in sources},
        docker=json.loads(
            subprocess.check_output(
                ["docker", "info", "--format", "{{json .}}"], text=True
            )
        )["ServerVersion"],
    )
    (output / "environment.json").write_text(json.dumps(meta, indent=2))
    call("build", "embedding")
    call("up", "-d", "--wait", "mysql", "kafka")
    pause = folder / "capacity" / "pause"
    paused = False
    try:
        if (folder / "capacity" / "state.json").exists() and not (
            folder / "capacity" / "full.json"
        ).exists():
            pause.touch(exist_ok=False)
            paused = True
            deadline = time.monotonic() + 90
            while not (folder / "capacity" / "paused.json").exists():
                if time.monotonic() > deadline:
                    raise RuntimeError("Capacity importer did not acknowledge pause")
                time.sleep(1)
        for trial in range(1, 4):
            schemas = [f"{run}_t{trial}_{mode}" for mode in MODES]
            sql = "\n".join(
                f"CREATE DATABASE {db}; GRANT ALL ON {db}.* TO 'papersearch'@'%';"
                for db in schemas
            )
            call(
                "exec",
                "-T",
                "mysql",
                "sh",
                "-c",
                'export MYSQL_PWD="$MYSQL_ROOT_PASSWORD"; exec mysql -uroot',
                input=sql,
                text=True,
            )
            call(
                "run",
                "--rm",
                "--no-deps",
                "--entrypoint",
                "python",
                "-v",
                f"{ROOT / 'scripts'}:/verify:ro",
                "-v",
                f"{args.models.resolve()}:/models:ro",
                "-e",
                "PAPERSEARCH_MODEL_DIR=/models/bge-small-en-v1.5",
                "embedding",
                "-u",
                "/verify/controlled-experiments.py",
                run,
                str(trial),
            )
        (output / "completed.json").write_text(
            json.dumps(dict(end=time.time(), trials=3))
        )
    finally:
        if paused:
            pause.unlink(missing_ok=True)
        call("stop", "mysql", "kafka")


if __name__ == "__main__":
    main()
