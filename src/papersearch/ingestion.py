# AI-generated code (Codex): ordered durable processing and explicit consumer group offsets.
import hashlib
import os
import re
import time
from collections import Counter
from contextlib import closing
from pathlib import Path

from kafka.consumer.subscription_state import ConsumerRebalanceListener
from kafka.structs import OffsetAndMetadata
from sqlalchemy import event as sql_event
from sqlalchemy.exc import DBAPIError

from .adapters import json_record
from .archive import write_atomic
from .contracts import RecordError, parse_envelope, provenance
from .broker import group_reader, log, offsets, topic_info
from .storage.metadata import apply_event, apply_fresh_events, reject_record


def apply_message(engine, record, quarantine: Path):
    raw = record.value
    try:
        row = json_record(raw) if raw is not None else {}
        if "source_rejection" in row:
            checksum, fmt, reason = (
                row.get("raw_sha256"),
                row.get("format"),
                row.get("reason"),
            )
            if (
                type(row["source_rejection"]) is not int
                or row["source_rejection"] != 1
                or fmt not in ("event", "arxiv", "fixture")
                or not isinstance(checksum, str)
                or not re.fullmatch("[a-f0-9]{64}", checksum)
                or record.key != checksum.encode()
                or not isinstance(reason, str)
                or not re.fullmatch("[a-z_]{1,64}", reason)
            ):
                raise RecordError("invalid_rejection")
            location = provenance(row.get("raw_location"), 4096)
            reject_record(engine, checksum, fmt, reason, location)
            return "rejected", checksum
        event = parse_envelope(row)
        if record.key != event.paper_id.encode():
            raise RecordError("paper_key_mismatch")
    except RecordError as exc:
        data = raw or b""
        checksum = hashlib.sha256(data).hexdigest()
        quarantine.mkdir(parents=True, exist_ok=True)
        path = quarantine / checksum
        write_atomic(path, data)
        fmt = "kafka" if raw is not None else "kafka-null"
        reject_record(engine, checksum, fmt, str(exc), str(path.resolve()))
        return "rejected", checksum
    return apply_event(engine, event), event.canonical()["event_id"]


def apply_delivery_batch(engine, records, quarantine):
    """Apply contiguous fresh spans in bulk and resolve exceptional deliveries in order."""
    if not 1 <= len(records) <= 256:
        raise ValueError("Expected 1 to 256 deliveries")
    counts = Counter()
    pending = []

    def flush():
        if not pending:
            return
        if apply_fresh_events(engine, [event for _, event in pending]):
            counts["applied"] += len(pending)
        else:
            counts.update(
                apply_message(engine, record, quarantine)[0] for record, _ in pending
            )
        pending.clear()

    for record in records:
        try:
            event = parse_envelope(json_record(record.value))
            if record.key != event.paper_id.encode():
                raise RecordError("paper_key_mismatch")
        except RecordError:
            flush()
            counts[apply_message(engine, record, quarantine)[0]] += 1
        else:
            pending.append((record, event))
    flush()
    return counts


class Assignment(ConsumerRebalanceListener):
    def __init__(self):
        self.next_offsets = {}

    def on_partitions_revoked(self, revoked):
        self.next_offsets.clear()

    def on_partitions_assigned(self, assigned):
        self.next_offsets.clear()


def consume(
    engine,
    broker,
    topic,
    group,
    quarantine,
    *,
    drain=False,
    fault=None,
    at=None,
    drain_signal=None,
    on_commit=None,
):
    if fault or at:
        if os.environ.get("PAPERSEARCH_TEST_FAULTS") != "1" or not (
            fault in ("before-commit", "after-commit")
            and at
            and re.fullmatch(r"[0-2]:[0-9]+", at)
        ):
            raise ValueError(
                "Fault injection requires explicit test mode, boundary and partition:offset"
            )
    topic_info(broker, topic)
    counts = Counter()
    with closing(group_reader(broker, group)) as reader:
        initial = offsets(reader, topic)
        log("group_start", topic=topic, group=group, partitions=initial)
        assignment = Assignment()
        reader.subscribe([topic], listener=assignment)
        while True:
            polled = reader.poll(timeout_ms=1000)
            if not polled:
                state = offsets(reader, topic)
                if (drain or (drain_signal and drain_signal())) and all(
                    p["lag"] == 0 for p in state.values()
                ):
                    log("drained", group=group, partitions=state, outcomes=dict(counts))
                    return
                continue
            for part, records in polled.items():
                if part not in assignment.next_offsets:
                    assignment.next_offsets[part] = reader.committed(part) or 0
                for record in records:
                    if record.offset != assignment.next_offsets[part]:
                        raise ValueError(
                            f"Noncontiguous delivery on {part}: expected "
                            f"{assignment.next_offsets[part]}, received {record.offset}"
                        )
                    where = dict(
                        group=group,
                        topic=topic,
                        partition=part.partition,
                        offset=record.offset,
                    )
                    inject = at == f"{part.partition}:{record.offset}"

                    def crash(*_):
                        log("test_crash", boundary=fault, **where)
                        os._exit(86)

                    if inject and fault == "before-commit":
                        sql_event.listen(engine, "commit", crash, once=True)
                    for attempt in range(1, 5):
                        try:
                            outcome, identity = apply_message(
                                engine, record, quarantine
                            )
                            break
                        except DBAPIError as exc:
                            log(
                                "sql_error",
                                attempt=attempt,
                                error=type(exc.orig).__name__,
                                errno=getattr(exc.orig, "errno", None),
                                **where,
                            )
                            if attempt == 4:
                                raise
                            time.sleep(0.25 * 2 ** (attempt - 1))
                    if inject and fault == "after-commit":
                        crash()
                    following = record.offset + 1
                    reader.commit(
                        {part: OffsetAndMetadata(following, "", -1)}, timeout_ms=10000
                    )
                    assignment.next_offsets[part] = following
                    counts[outcome] += 1
                    if on_commit is not None:
                        on_commit(record, outcome)
                    log(
                        "offset_commit",
                        next_offset=following,
                        outcome=outcome,
                        identity=identity,
                        **where,
                    )
