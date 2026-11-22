# AI-generated measurement (Codex): sample the two persistent Docker volumes and WSL VM memory.
import argparse
import json
import subprocess
import time
from pathlib import Path


def sample(folder):
    state = json.loads((folder / "capacity/state.json").read_bytes())
    result = {"time": time.time(), "completed_blocks": state["blocks"]}
    for service, mount in (
        ("mysql", "/var/lib/mysql"),
        ("kafka", "/var/lib/kafka/data"),
    ):
        prefix = ["docker", "exec", f"papersearch-m8-capacity-{service}-1"]
        usage = subprocess.check_output([*prefix, "du", "-sk", mount], text=True)
        result[service + "_volume_bytes"] = int(usage.split()[0]) * 1024
        if service == "mysql":
            mem = subprocess.check_output([*prefix, "cat", "/proc/meminfo"], text=True)
            kib = {row.split(":")[0]: int(row.split()[1]) for row in mem.splitlines()}
            result["vm_used_bytes"] = (kib["MemTotal"] - kib["MemAvailable"]) * 1024
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("folder", type=Path)
    args = parser.parse_args()
    with (args.folder / "volume-samples.jsonl").open("a") as out:
        missed = 0
        while not (args.folder / "capacity/full.json").exists():
            try:
                row = sample(args.folder)
            except subprocess.CalledProcessError:
                missed += 1
                if missed >= 5:
                    raise
            else:
                missed = 0
                out.write(json.dumps(row) + "\n")
                out.flush()
            time.sleep(60)
