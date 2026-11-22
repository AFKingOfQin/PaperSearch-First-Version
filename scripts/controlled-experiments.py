# AI-generated experiments (Codex): compare isolated real-service ingestion, recovery, caching and publication.
import argparse
import csv
import json
import os
import subprocess
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from statistics import median

from alembic import command
from alembic.config import Config
from kafka.admin import KafkaAdminClient
from sqlalchemy import create_engine, text

from papersearch.adapters import json_record, parse_record, raw_lines
from papersearch.archive import archive, write_atomic
from papersearch.broker import group_reader, offsets, topic_info
from papersearch.config import mysql_url
from papersearch.contracts import digest
from papersearch.derivation import bind_store, configure_recipe, process_batch
from papersearch.encoder import Encoder, RECIPE, eligible
from papersearch.ingestion import consume
from papersearch.publication import publish
from papersearch.reference import process_files
from papersearch.snapshot_format import file_sha
from papersearch.snapshots import publish_snapshot
from papersearch.storage.metadata import export_rows

BROKER = "kafka:9092"
STAMP = "2026-11-21T00:00:00Z"
MODES = (
    "full",
    "incremental",
    "bypass",
    "derived2",
    "workers1",
    "workers2",
    "workers3",
    "before",
    "after",
)


def save(path, value):
    write_atomic(path, json.dumps(value, sort_keys=True).encode())


def inputs(root):
    events = []
    workload = []
    source = Path("/artifacts/input/blocks/00001.jsonl")
    for line, sha, raw in raw_lines(source):
        event = parse_record(
            json_record(raw),
            "arxiv",
            batch_id="source",
            collected_at=STAMP,
            location=f"v287#{line}",
            checksum=sha,
        )
        if len(workload) < 384:
            workload.append(event)
        if eligible(event.paper_id) and len(events) < 56:
            events.append(event)
        if len(events) == 56 and len(workload) == 384:
            break
    base = events[:48]
    update = []
    for i, e in enumerate(base[:12]):
        payload = (
            replace(e.payload, abstract=e.payload.abstract + " Controlled revision.")
            if i < 8
            else replace(e.payload, authors=(*e.payload.authors, "Fixture Author"))
        )
        update.append(
            replace(e, source_version=(e.source_version or 0) + 1, payload=payload)
        )
    for i, e in enumerate(events[48:]):
        if i < 4:
            e = replace(
                e,
                payload=replace(
                    e.payload,
                    title=base[i].payload.title,
                    abstract=base[i].payload.abstract,
                ),
            )
        update.append(e)
    update.extend(base[:2])
    manifests = {}
    for label, rows in (
        ("base", base),
        ("delta", update),
        ("all", base + update),
        ("stream", workload),
    ):
        path = root / (label + ".jsonl")
        with path.open("x", encoding="utf-8", newline="\n") as out:
            for e in rows:
                out.write(json.dumps(e.envelope(), ensure_ascii=False) + "\n")
        manifests[label] = dict(
            rows=len(rows), sha256=file_sha(path), bytes=path.stat().st_size
        )
    result = dict(
        source_block_sha256=file_sha(source), inputs=manifests, seed=None, oracle={}
    )
    for name in ("all", "stream"):
        result["oracle"][name] = process_files(
            [root / (name + ".jsonl")], "event", "", STAMP
        )["state_sha256"]
    save(root / "inputs.json", result)


def engine_for(name):
    return create_engine(
        mysql_url({**os.environ, "MYSQL_DATABASE": name}), pool_pre_ping=True
    )


def sql_state(engine):
    with engine.connect() as db:
        counts = {
            t: db.scalar(text("SELECT COUNT(*) FROM " + t))
            for t in (
                "current_revisions",
                "processed_events",
                "paper_revisions",
                "rejected_events",
            )
        }
        counts["generation"] = db.scalar(text("SELECT generation FROM system_state"))
    return counts


def send(root, label, topic, group):
    folder = archive(
        root / (label + ".jsonl"), root / "archives", "event", label, STAMP
    )
    publish(BROKER, topic, folder, group=group)


def derive(engine, store, encoder, workers, cache):
    bind_store(engine, store)

    def work():
        total = Counter()
        while True:
            batch = process_batch(engine, store, 1, 180, encoder, use_cache=cache)
            total.update(batch)
            total["model_calls"] += int(batch.get("computed", 0) > 0)
            if batch.get("errors") or batch.get("lost_leases"):
                raise RuntimeError(batch)
            if not batch["claimed"]:
                return total

    with ThreadPoolExecutor(max_workers=workers) as pool:
        counts = sum(pool.map(lambda _: work(), range(workers)), Counter())
    return dict(counts)


def pipeline(root, folder, name, mode, oracle):
    engine = engine_for(name)
    store = folder / "store"
    encoder = Encoder(threads=1)
    topic_info(BROKER, name, create=True)

    def ingest(label):
        send(root, label, name, name)
        ack = time.time()
        consume(engine, BROKER, name, name, folder / "quarantine", drain=True)
        return ack

    if mode != "full":
        ingest("base")
        configure_recipe(engine, RECIPE)
        derive(engine, store, encoder, 1, True)
        publish_snapshot(engine, store)
    before = sql_state(engine)
    old_bytes = sum(
        p.stat().st_size for p in (store / "shards").rglob("*") if p.is_file()
    )
    cpu = time.process_time()
    t = time.time()
    label = "all" if mode == "full" else "delta"
    ack = ingest(label)
    durable = time.time()
    configure_recipe(engine, RECIPE)
    start = time.time()
    counts = derive(
        engine, store, encoder, 2 if mode == "derived2" else 1, mode != "bypass"
    )
    embedded = time.time()
    manifest = publish_snapshot(engine, store)
    visible = time.time()
    after = sql_state(engine)
    if manifest["corpus_sha256"] != oracle or manifest["coverage"]["semantic"] != 56:
        raise ValueError("Pipeline corpus/coverage mismatch")
    import pyarrow.parquet as pq

    artifact = store / "snapshots" / manifest["snapshot_id"] / "semantic.parquet"
    with pq.ParquetFile(artifact) as f:
        vectors = digest(
            [
                [r["paper_id"], r["cache_key"], r["vector_sha"]]
                for b in f.iter_batches(columns=["paper_id", "cache_key", "vector_sha"])
                for r in b.to_pylist()
            ]
        )
    result = dict(
        start=t,
        ack=ack,
        durable=durable,
        derive_start=start,
        embedded=embedded,
        visible=visible,
        elapsed_seconds=visible - t,
        derive_seconds=embedded - start,
        cpu_seconds=time.process_time() - cpu,
        inputs=70 if mode == "full" else 22,
        db_before=before,
        db_after=after,
        counts=counts,
        shard_bytes_added=sum(
            p.stat().st_size for p in (store / "shards").rglob("*") if p.is_file()
        )
        - old_bytes,
        current_digest=oracle,
        vector_digest=vectors,
        coverage=manifest["coverage"],
        package_bytes=sum(v["bytes"] for v in manifest["files"].values()),
    )
    engine.dispose()
    return result


def worker(folder, name, idx, fault):
    with (folder / f"{idx}.csv").open("x", buffering=1) as out:
        writer = csv.writer(out)

        def committed(record, outcome):
            writer.writerow(
                [
                    time.time(),
                    record.timestamp,
                    record.partition,
                    record.offset,
                    outcome,
                ]
            )

        consume(
            engine_for(name),
            BROKER,
            name,
            name,
            folder / "quarantine",
            drain_signal=lambda: (folder / "published").exists(),
            on_commit=committed,
            fault=fault,
            at="0:16" if fault else None,
        )


def stream(root, folder, name, mode, oracle):
    number = int(mode[-1]) if mode.startswith("workers") else 1
    fault = {"before": "before-commit", "after": "after-commit"}.get(mode)
    topic_info(BROKER, name, create=True)
    children = []
    samples = []
    finished = threading.Event()

    def launch(idx, point=None):
        args = [sys.executable, __file__, "worker", str(folder), name, str(idx)]
        if point:
            args.append(point)
        with (folder / f"{idx}.log").open("w") as out:
            child = subprocess.Popen(
                args,
                stdout=out,
                stderr=subprocess.STDOUT,
                env={**os.environ, "PAPERSEARCH_TEST_FAULTS": "1"},
            )
        children.append(child)
        return child

    def sample():
        with closing(group_reader(BROKER, name)) as observer:
            while not finished.is_set():
                lag = sum(p["lag"] for p in offsets(observer, name).values())
                cpu = int(
                    Path("/sys/fs/cgroup/cpu.stat")
                    .read_text()
                    .splitlines()[0]
                    .split()[1]
                )
                samples.append(
                    [
                        time.time(),
                        lag,
                        cpu,
                        int(Path("/sys/fs/cgroup/memory.current").read_text()),
                    ]
                )
                finished.wait(0.3)

    monitor = None
    recovery = None
    try:
        for i in range(number):
            launch(i, fault)
        deadline = time.monotonic() + 80
        with closing(KafkaAdminClient(bootstrap_servers=BROKER)) as admin:
            while True:
                info = admin.describe_consumer_groups([name])[0]
                if info.state == "Stable" and len(info.members) == number:
                    break
                if time.monotonic() > deadline or any(
                    p.poll() is not None for p in children
                ):
                    raise RuntimeError(f"Workers did not join {name}")
                time.sleep(0.2)
        monitor = threading.Thread(target=sample)
        monitor.start()
        start = time.time()
        send(root, "stream", name, name)
        ack = time.time()
        (folder / "published").touch()
        if fault:
            assert children[0].wait(timeout=60) == 86
            crash_observed = time.time()
            with closing(group_reader(BROKER, name)) as obs:
                remaining = offsets(obs, name)
            launch(number)
            recovery = dict(
                crash_observed=crash_observed, positions_at_restart=remaining
            )
        for child in children[1 if fault else 0 :]:
            if child.wait(timeout=120):
                raise RuntimeError(f"Worker failed: {folder}")
    finally:
        finished.set()
        if monitor:
            monitor.join(timeout=15)
        for child in children:
            if child.poll() is None:
                child.terminate()
                child.wait(timeout=15)
    groups = []
    all_rows = []
    for path in sorted(folder.glob("*.csv")):
        with path.open() as f:
            rows = list(csv.reader(f))
        groups.append(
            dict(
                worker=path.stem,
                commits=len(rows),
                partitions=sorted({int(r[2]) for r in rows}),
            )
        )
        all_rows.extend(rows)
    durable = max(float(r[0]) for r in all_rows)
    latencies = [float(r[0]) * 1000 - int(r[1]) for r in all_rows]
    engine = engine_for(name)
    exported = export_rows(engine)
    next(exported)
    corpus = digest({row["paper_id"]: row for row in exported})
    state = sql_state(engine)
    engine.dispose()
    with closing(group_reader(BROKER, name)) as obs:
        end = offsets(obs, name)
    outcomes = Counter(r[4] for r in all_rows)
    if (
        any(p["lag"] for p in end.values())
        or corpus != oracle
        or state["processed_events"] != 384
        or len(all_rows) != 384
    ):
        raise ValueError(
            f"Stream reconciliation failed {name}: {state} {outcomes} {groups}"
        )
    if fault == "after-commit" and outcomes["duplicate"] != 1:
        raise ValueError("Expected exactly one durable redelivery")
    if recovery:
        recovery["restart_through_last_commit_seconds"] = (
            durable - recovery["crash_observed"]
        )
    if len(samples) < 2:
        raise ValueError("Missing resource samples")
    return dict(
        start=start,
        ack=ack,
        durable=durable,
        elapsed_seconds=durable - start,
        rows_per_second=384 / (durable - start),
        committed_latency_ms_median=median(latencies),
        outcomes=dict(outcomes),
        resources=samples,
        sample_columns=[
            "wall_epoch",
            "lag",
            "client_cgroup_cpu_usec",
            "client_cgroup_memory_bytes",
        ],
        workers=groups,
        offsets=end,
        state=state,
        current_digest=corpus,
        recovery=recovery,
    )


def main():
    if sys.argv[1] == "worker":
        worker(
            Path(sys.argv[2]),
            sys.argv[3],
            int(sys.argv[4]),
            sys.argv[5] if len(sys.argv) > 5 else None,
        )
        return
    p = argparse.ArgumentParser()
    p.add_argument("run")
    p.add_argument("trial", type=int)
    args = p.parse_args()
    root = Path("/artifacts") / args.run
    root.mkdir(exist_ok=True)
    if not (root / "inputs.json").exists():
        inputs(root)
    data = json.loads((root / "inputs.json").read_bytes())
    modes = list(MODES)
    rotate = 3 * (args.trial - 1)
    for mode in modes[rotate:] + modes[:rotate]:
        name = f"{args.run}_t{args.trial}_{mode}"
        folder = root / name
        if (folder / "result.json").exists():
            continue
        folder.mkdir()
        os.environ["MYSQL_DATABASE"] = name
        command.upgrade(Config("/app/alembic.ini"), "head")
        if mode in MODES[:4]:
            result = pipeline(root, folder, name, mode, data["oracle"]["all"])
        else:
            result = stream(root, folder, name, mode, data["oracle"]["stream"])
        save(folder / "result.json", dict(trial=args.trial, mode=mode, **result))
        print("MEASURED", name, result["elapsed_seconds"], flush=True)


if __name__ == "__main__":
    main()
