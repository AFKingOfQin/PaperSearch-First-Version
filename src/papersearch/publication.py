# AI-generated code (Codex): keyed Kafka publication with broker-confirmed archive checkpoints.
import hashlib
import json
import time
from contextlib import closing
from pathlib import Path

from kafka import KafkaConsumer, KafkaProducer, TopicPartition

from .adapters import json_record, parse_record, raw_lines
from .archive import inspect_batch, write_atomic
from .broker import group_reader, log, offsets, topic_info
from .contracts import RecordError, digest


def message(manifest, path, line, checksum, raw):
    location = f"{path.as_posix()}#{line}"
    try:
        event = parse_record(
            json_record(raw),
            manifest["format"],
            batch_id=manifest["batch_id"],
            collected_at=manifest["collected_at"],
            location=location,
            checksum=checksum,
        )
    except RecordError as exc:
        key = checksum
        value = dict(
            source_rejection=1,
            reason=str(exc),
            raw_sha256=checksum,
            raw_location=location,
            format=manifest["format"],
        )
    else:
        key, value = event.paper_id, event.envelope()
    return key.encode(), json.dumps(
        value, ensure_ascii=False, separators=(",", ":")
    ).encode()


def verify_ack(broker, topic, checkpoint):
    part = TopicPartition(topic, checkpoint["partition"])
    with closing(
        KafkaConsumer(
            bootstrap_servers=broker,
            group_id=None,
            enable_auto_commit=False,
            auto_offset_reset="none",
        )
    ) as reader:
        reader.assign([part])
        offset = checkpoint["offset"]
        if (
            not reader.beginning_offsets([part])[part]
            <= offset
            < reader.end_offsets([part])[part]
        ):
            raise ValueError(
                "Publication checkpoint is outside retained Kafka data. Replay the archive."
            )
        reader.seek(part, offset)
        until = time.monotonic() + 10
        while time.monotonic() < until:
            rows = reader.poll(timeout_ms=500, max_records=1).get(part, [])
            if rows:
                record = rows[0]
                if (
                    record.offset == offset
                    and record.value is not None
                    and hashlib.sha256(record.value).hexdigest()
                    == checkpoint["value_sha256"]
                ):
                    return
                break
        raise ValueError(
            "Broker record does not match the publication checkpoint. Replay the archive."
        )


def publish(
    broker: str,
    topic: str,
    directory: Path,
    replay=False,
    *,
    group="papersearch-ingest",
    max_lag=4096,
):
    if max_lag < 1:
        raise ValueError("max-lag must be positive")
    directory = directory.resolve()
    manifest = inspect_batch(directory)
    cluster = topic_info(broker, topic)
    checkpoint_file = directory / f"ack-{digest([cluster, topic])}.json"
    start = 0
    if replay:
        write_atomic(checkpoint_file, b'{"line":0}')
    elif checkpoint_file.exists():
        checkpoint = json.loads(checkpoint_file.read_bytes())
        start = checkpoint["line"]
        if type(start) is not int or not 0 <= start <= manifest["records"]:
            raise ValueError("Invalid acknowledged line count")
        if start:
            verify_ack(broker, topic, checkpoint)
    path = directory / "records.jsonl"
    pending = []
    with (
        closing(group_reader(broker, group)) as monitor,
        closing(
            KafkaProducer(
                bootstrap_servers=broker,
                acks="all",
                retries=3,
                max_in_flight_requests_per_connection=1,
                max_block_ms=10000,
            )
        ) as producer,
    ):
        for line, checksum, raw in raw_lines(path):
            if line == start:
                _, last_value = message(manifest, path, line, checksum, raw)
                if hashlib.sha256(last_value).hexdigest() != checkpoint["value_sha256"]:
                    raise ValueError(
                        "Acknowledged line does not match the archive. Replay the archive."
                    )
            if line <= start:
                continue
            if not pending:
                lag = sum(p["lag"] for p in offsets(monitor, topic).values())
                window = min(32, max_lag, manifest["records"] - line + 1)
                if lag + window > max_lag:
                    log(
                        "publication_paused",
                        topic=topic,
                        group=group,
                        lag=lag,
                        max_lag=max_lag,
                        acknowledged=line - 1,
                    )
                    raise ValueError(
                        "Consumer lag limit reached. Drain the group and resume publication."
                    )
            key, value = message(manifest, path, line, checksum, raw)
            pending.append(producer.send(topic, key=key, value=value))
            if len(pending) == window:
                for future in pending:
                    ack = future.get(timeout=30)
                checkpoint = dict(
                    line=line,
                    partition=ack.partition,
                    offset=ack.offset,
                    value_sha256=hashlib.sha256(value).hexdigest(),
                )
                write_atomic(checkpoint_file, json.dumps(checkpoint).encode())
                log("publication_ack", topic=topic, batch=directory.name, **checkpoint)
                pending.clear()
    log(
        "publication_complete",
        topic=topic,
        archive=directory.name,
        records=manifest["records"],
        resumed_after=start,
        published=manifest["records"] - start,
    )
