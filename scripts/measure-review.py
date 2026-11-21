# AI-generated code (Codex): observe a published gate through the API and measure the serving containers.
import argparse
import json
import subprocess
import time
from pathlib import Path

import psutil
import requests

from papersearch.config import settings
from review_demo import REPO, wait_http, workflow


def measure(project, size, output):
    def compose(*args):
        return subprocess.check_output(
            [
                "docker",
                "compose",
                "-p",
                project,
                "--profile",
                "core",
                "--profile",
                "review",
                *args,
            ],
            text=True,
        ).strip()

    def inside(service, *args):
        return compose("exec", "-T", service, *args)

    def memory():
        line = inside("api", "cat", "/proc/1/status")
        values = {
            k: int(v.split()[0]) * 1024
            for k, v in (r.split(":", 1) for r in line.splitlines())
            if k in ("VmRSS", "VmHWM")
        }
        values["cgroup_peak_bytes"] = int(
            inside("api", "cat", "/sys/fs/cgroup/memory.peak")
        )
        return values

    def restart():
        compose("restart", "mysql", "api", "ui")
        compose("up", "-d", "--no-build", "--pull", "never", "--wait", "mysql")
        wait_http(base + "/ready")
        wait_http(browser + "/_stcore/health")

    output.mkdir(parents=True, exist_ok=True)
    base = "http://" + compose("port", "api", "8000")
    browser = "http://" + compose("port", "ui", "8501")
    compose("restart", "api")
    wait_http(base + "/health")
    before = memory()
    start = time.perf_counter()
    ready = requests.get(base + "/ready", timeout=120)
    ready.raise_for_status()
    cold_ms = 1000 * (time.perf_counter() - start)
    loaded = memory()
    status = requests.get(base + "/status", timeout=30).json()
    manifest = status["active"]
    assert manifest["snapshot_id"] == ready.json()["snapshot_id"]
    assert manifest["coverage"]["lexical"] == size
    review = requests.post(
        base + "/projects", json={"topic": f"BM25 serving check {size}"}, timeout=10
    )
    review.raise_for_status()
    timings, paper = [], None
    for _ in range(5):
        t = time.perf_counter()
        search = requests.get(
            f"{base}/projects/{review.json()['project_id']}/search",
            params=dict(q="quantum", limit=10, snapshot=manifest["snapshot_id"]),
            timeout=30,
        )
        search.raise_for_status()
        timings.append((time.perf_counter() - t) * 1000)
        hits = search.json()["results"]
        assert len(hits) == 10 and (paper is None or paper == hits[0]["paper_id"])
        paper = hits[0]["paper_id"]
    report = dict(
        gate=size,
        snapshot_id=manifest["snapshot_id"],
        query="quantum",
        first_paper=paper,
        api_load_ms=cold_ms,
        api_query_ms=timings,
        before_index=before,
        loaded_index=loaded,
        after_serving=memory(),
        index_bytes=manifest["files"]["lexical.sqlite"]["bytes"],
        mysql_dir_bytes=int(inside("mysql", "du", "-sb", "/var/lib/mysql").split()[0]),
        kafka_dir_bytes=int(
            inside("kafka", "du", "-sb", "/var/lib/kafka/data").split()[0]
        ),
        service_lifetime_peak_bytes={
            s: int(inside(s, "cat", "/sys/fs/cgroup/memory.peak"))
            for s in ("mysql", "kafka")
        },
        docker_host_free_bytes=psutil.disk_usage(
            settings(REPO / ".env")["DOCKER_DISK_DIR"]
        ).free,
        observed_at=time.time(),
    )
    for proc in psutil.process_iter(["name"]):
        if proc.info["name"].lower() == "vmmemwsl":
            mem = proc.memory_info()
            report["wsl_working_set_bytes"] = mem.rss
            report["wsl_lifetime_peak_bytes"] = mem.peak_wset
    if size == 100000:
        assert status["metadata_generations_behind"] == status["broker"]["lag"] == 0
        report["review_project"] = workflow(base, output, restart, query="quantum")
    (output / f"serve-{size}.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("project")
    parser.add_argument("size", type=int, choices=[10000, 50000, 100000])
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    measure(args.project, args.size, args.output)
