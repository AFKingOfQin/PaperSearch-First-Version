# AI-generated experiment (Codex): checkpointed Kafka-to-MySQL capacity gates on an isolated metadata store.
import argparse
import hashlib
import json
import os
import shutil
import time
from collections import Counter
from contextlib import closing
from pathlib import Path
from uuid import uuid4

from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata
from sqlalchemy import create_engine, text

from papersearch.adapters import raw_lines
from papersearch.archive import write_atomic
from papersearch.broker import offsets, topic_info
from papersearch.config import mysql_url
from papersearch.ingestion import apply_delivery_batch
from papersearch.publication import message
from papersearch.snapshot_format import file_sha
from papersearch.storage.reviews import create_project, read_decision, save_decision

ROOT = Path("/artifacts")
SOURCE = ROOT / "input"
RUN = ROOT / "capacity"
TOPIC = "metadata-v287"
GROUP = "capacity-v287"
STAMP = "2026-11-21T00:00:00Z"
BROKER = "kafka:9092"


def save(path, data):
    write_atomic(path, json.dumps(data, sort_keys=True).encode())


def drain(engine, reader, parts, targets):
    counts = Counter()
    current = {p: reader.committed(p) or 0 for p in parts}
    for p in parts:
        reader.seek(p, current[p])
    while any(current[p] < targets[p] for p in parts):
        for p, records in reader.poll(timeout_ms=1000, max_records=256).items():
            if records[0].offset != current[p] or records[-1].offset + 1 != current[
                p
            ] + len(records):
                raise ValueError("Noncontiguous capacity delivery")
            # Bound SQL packets even for unusually long abstracts.
            block = []
            size = 0
            for r in records:
                if block and size + len(r.value or b"") > 1024 * 1024:
                    counts.update(
                        apply_delivery_batch(engine, block, RUN / "quarantine")
                    )
                    block = []
                    size = 0
                block.append(r)
                size += len(r.value or b"")
            counts.update(apply_delivery_batch(engine, block, RUN / "quarantine"))
            current[p] = records[-1].offset + 1
            reader.commit({p: OffsetAndMetadata(current[p], "", -1)}, timeout_ms=20000)
    return counts


def submit(producer, path, info, start, ack_path):
    manifest = dict(format="arxiv", batch_id=path.stem, collected_at=STAMP)
    futures = []
    for line, checksum, raw in raw_lines(path):
        if line <= start:
            continue
        key, value = message(manifest, path, line, checksum, raw)
        futures.append(producer.send(TOPIC, key=key, value=value))
        if len(futures) == 256:
            for f in futures:
                f.get(timeout=45)
            save(ack_path, dict(line=line, sha256=info["sha256"]))
            futures.clear()
    for f in futures:
        f.get(timeout=45)
    save(ack_path, dict(line=line, sha256=info["sha256"]))


def gate(engine, reader, state, name, expected):
    h = hashlib.sha256()
    count = 0
    last = ""
    with engine.connect() as db:
        while rows := db.execute(
            text("""SELECT c.paper_id,c.event_id,r.text_hash FROM
          (SELECT paper_id,event_id FROM current_revisions WHERE paper_id>:last ORDER BY paper_id LIMIT 4096) c
          JOIN paper_revisions r ON r.event_id=c.event_id ORDER BY c.paper_id"""),
            dict(last=last),
        ).all():
            for row in rows:
                h.update(("\t".join(row) + "\n").encode())
                last = row[0]
            count += len(rows)
        totals = {
            t: db.scalar(text("SELECT COUNT(*) FROM " + t))
            for t in (
                "processed_events",
                "rejected_events",
                "derived_jobs",
                "screening_history",
                "published_snapshots",
            )
        }
        reasons = dict(
            db.execute(
                text("SELECT reason,COUNT(*) FROM rejected_events GROUP BY reason")
            ).all()
        )
        db.execute(text("SET SESSION information_schema_stats_expiry=0"))
        db_bytes = int(
            db.scalar(
                text(
                    "SELECT SUM(data_length+index_length) FROM information_schema.tables WHERE table_schema=DATABASE()"
                )
            )
        )
    decision = read_decision(engine, state["project"], state["paper"])
    decision["saved_at"] = decision["saved_at"].isoformat()
    positions = offsets(reader, TOPIC)
    if (
        h.hexdigest() != expected["identity_sha256"]
        or count != expected["current"]
        or totals["processed_events"] != expected["distinct_valid_events"]
        or reasons != expected["rejected"]
        or sum(p["end"] for p in positions.values()) != expected["lines"]
        or decision["reviewed_event_id"] != state["reviewed"]
        or decision["note"] != "Capacity restart check"
        or decision["label"] != "relevant"
        or totals["screening_history"] != 1
        or any(p["lag"] for p in positions.values())
    ):
        raise ValueError("Capacity reconciliation failed")
    result = dict(
        gate=name,
        expected=expected,
        current=count,
        identity_sha256=h.hexdigest(),
        tables=totals,
        rejection_reasons=reasons,
        mysql_table_bytes=db_bytes,
        offsets=positions,
        decision=decision,
        first_started=state["started"],
        verified=time.time(),
        lexical=0,
        semantic=0,
        metadata_only=True,
        attempts=state["attempts"],
        blocks=state["blocks"],
    )
    save(RUN / (name + ".json"), result)
    print(name, count, h.hexdigest(), flush=True)


def recovered_block(engine, idx, broker_ends):
    """After a partial-block restart, recover net source outcomes from durable ledgers."""
    previous = Counter()
    for number in range(1, idx):
        block = json.loads((RUN / "logs" / f"block-{number:05d}.json").read_bytes())
        if block.get("net_outcomes") is not None:
            previous.update(block["net_outcomes"])
        else:
            previous.update(block["outcomes"])
            previous.update(block["resumed_outcomes"])
    with engine.connect() as db:
        totals = Counter(
            dict(
                db.execute(
                    text(
                        "SELECT outcome,COUNT(*) FROM processed_events GROUP BY outcome"
                    )
                ).all()
            )
        )
        totals["rejected"] = db.scalar(text("SELECT COUNT(*) FROM rejected_events"))
    # The pinned input has no equal-priority conflicts, and every rejection has a distinct raw hash.
    totals["duplicate"] = sum(broker_ends.values()) - sum(totals.values())
    totals.subtract(previous)
    if any(n < 0 for n in totals.values()):
        raise ValueError("Durable outcome totals went backwards")
    return dict(+totals)


def run(stop_after):
    engine = create_engine(mysql_url(os.environ), pool_pre_ping=True)
    cluster = topic_info(BROKER, TOPIC, create=True)
    RUN.mkdir(exist_ok=True)
    (RUN / "logs").mkdir(exist_ok=True)
    with (
        engine.connect() as lock,
        closing(
            KafkaProducer(
                bootstrap_servers=BROKER,
                acks="all",
                max_in_flight_requests_per_connection=1,
                retries=3,
                linger_ms=2,
                batch_size=131072,
            )
        ) as producer,
        closing(
            KafkaConsumer(
                bootstrap_servers=BROKER,
                group_id=GROUP,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                max_poll_records=256,
            )
        ) as reader,
    ):
        if lock.scalar(text("SELECT GET_LOCK('papersearch-m8-capacity',0)")) != 1:
            raise ValueError("Another capacity writer is active")
        server = lock.scalar(text("SELECT @@server_uuid"))
        statefile = RUN / "state.json"
        if statefile.exists():
            state = json.loads(statefile.read_bytes())
            if (cluster, server) != (state["cluster"], state["server"]):
                raise ValueError("Capacity storage identity changed")
        else:
            if lock.scalar(text("SELECT COUNT(*) FROM processed_events")) or sum(
                p["end"] for p in offsets(reader, TOPIC).values()
            ):
                raise ValueError("A new capacity run needs an empty database and topic")
            state = dict(
                cluster=cluster,
                server=server,
                started=time.time(),
                blocks=0,
                attempts=0,
            )
        state["attempts"] += 1
        save(statefile, state)
        parts = [TopicPartition(TOPIC, i) for i in range(3)]
        reader.assign(parts)
        while True:
            if (RUN / "pause").exists():
                save(RUN / "paused.json", dict(blocks=state["blocks"], at=time.time()))
                while (RUN / "pause").exists():
                    time.sleep(1)
                (RUN / "paused.json").unlink()
            for name, manifest in (
                ("million", SOURCE / "million.json"),
                ("full", SOURCE / "source.json"),
            ):
                if manifest.exists() and not (RUN / (name + ".json")).exists():
                    expected = json.loads(manifest.read_bytes())
                    if name == "full":
                        expected = expected["gates"]["full"]
                    if expected["blocks"] == state["blocks"]:
                        gate(engine, reader, state, name, expected)
            if (RUN / "full.json").exists():
                return
            idx = state["blocks"] + 1
            info_path = SOURCE / "blocks" / f"{idx:05d}.json"
            if not info_path.exists():
                time.sleep(2)
                continue
            free = shutil.disk_usage(ROOT).free
            if free < 100 * 2**30:
                raise RuntimeError("Artifact disk below 100 GiB reserve")
            info = json.loads(info_path.read_bytes())
            path = info_path.with_name(info["name"])
            if file_sha(path) != info["sha256"]:
                raise ValueError("Raw block checksum mismatch")
            t = time.time()
            ack_path = RUN / "logs" / f"ack-{idx}.json"
            ack = (
                json.loads(ack_path.read_bytes())
                if ack_path.exists()
                else dict(line=0, sha256=info["sha256"])
            )
            if ack["sha256"] != info["sha256"]:
                raise ValueError("Acknowledgement belongs to another source block")
            initial = offsets(reader, TOPIC)
            targets = {p: initial[p.partition]["end"] for p in parts}
            prior = drain(engine, reader, parts, targets)
            submit(producer, path, info, ack["line"], ack_path)
            acknowledged = time.time()
            targets = reader.end_offsets(parts)
            counts = drain(engine, reader, parts, targets)
            durable = time.time()
            net = (
                recovered_block(engine, idx, targets) if ack["line"] or prior else None
            )
            if "project" not in state:
                with engine.connect() as db:
                    paper, event = db.execute(
                        text(
                            "SELECT paper_id,event_id FROM current_revisions ORDER BY paper_id LIMIT 1"
                        )
                    ).one()
                project = create_project(
                    engine,
                    "v287 metadata capacity",
                    "Check decision through import restart",
                )
                save_decision(
                    engine,
                    project,
                    paper,
                    event,
                    "relevant",
                    "Capacity restart check",
                    str(uuid4()),
                )
                state.update(project=project, paper=paper, reviewed=event)
            state["blocks"] = idx
            save(
                RUN / "logs" / f"block-{idx:05d}.json",
                dict(
                    block=idx,
                    through=info["through"],
                    start=t,
                    ack=acknowledged,
                    durable=durable,
                    outcomes=dict(counts),
                    resumed_outcomes=dict(prior),
                    resumed_line=ack["line"],
                    net_outcomes=net,
                    partition_ends={p.partition: v for p, v in targets.items()},
                    artifact_free=free,
                    cgroup_peak=int(Path("/sys/fs/cgroup/memory.peak").read_text()),
                ),
            )
            save(statefile, state)
            if idx == stop_after:
                print("Injected process exit at durable block", idx, flush=True)
                os._exit(86)
            if idx % 10 == 0:
                print(idx, info["through"], round(durable - t, 2), flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--stop-after", type=int, default=0)
    run(p.parse_args().stop_after)
