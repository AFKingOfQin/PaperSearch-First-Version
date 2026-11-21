# AI-generated tests (Codex): compare independent MySQL databases across real Kafka process crashes.
import hashlib
import json
import os
import subprocess
import sys
from contextlib import closing
from pathlib import Path
from uuid import uuid4

from kafka import KafkaProducer
from sqlalchemy import create_engine, text

from papersearch.config import mysql_url
from papersearch.contracts import digest
from papersearch.storage.metadata import export_rows
from papersearch.storage.reviews import create_project, read_decision, save_decision

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "phase1"
EXPECTED = json.loads((FIXTURES / "expected.json").read_bytes())
TOPIC = "papersearch.updates"
STAMP = "2020-02-01T00:00:00Z"
SCENARIOS = ("baseline", "before", "after")
BAD = b'{"damaged wire'


def check(name, actual, expected):
    assert actual == expected, f"{name}: expected {expected!r}, got {actual!r}"
    print(f"PASS {name}: {actual!r}", flush=True)


def engine(scenario):
    return create_engine(mysql_url({**os.environ, "MYSQL_DATABASE": f"m3_{scenario}"}))


def command(work, name, *args, scenario="baseline", code=0, fault=False):
    env = {
        **os.environ,
        "MYSQL_DATABASE": f"m3_{scenario}",
        "PAPERSEARCH_TEST_FAULTS": "1" if fault else "0",
    }
    cmd = list(map(str, args))
    print(f"{name} [{scenario}] {' '.join(cmd)}", flush=True)
    result = subprocess.run(cmd, env=env, capture_output=True, timeout=150, text=True)
    (work / (name + ".log")).write_text(result.stdout + result.stderr)
    if result.returncode != code:
        raise AssertionError(
            f"{name}: exit {result.returncode}, expected {code}\n"
            + (result.stdout + result.stderr)[-2600:]
        )
    return result.stdout.splitlines()


def stream(work, name, *args, **kwargs):
    return command(work, name, "papersearch", "stream", *args, **kwargs)


def drain(work, name, scenario, *, group=None, extra=(), code=0, fault=False):
    lines = stream(
        work,
        name,
        "consume",
        TOPIC,
        "--group",
        group or scenario,
        "--drain",
        "--quarantine",
        work / "quarantine",
        *extra,
        scenario=scenario,
        code=code,
        fault=fault,
    )
    records = [json.loads(line) for line in lines]
    if records:
        print(json.dumps(records[-1], sort_keys=True), flush=True)
    return records


def status(work, name, group):
    lines = stream(work, name, "status", TOPIC, "--group", group)
    print(lines[-1], flush=True)
    return json.loads(lines[-1])["partitions"]


def publish_file(work, path, *, pause=False):
    (folder,) = stream(
        work,
        "archive-" + path.stem,
        "archive",
        path,
        "--archive-root",
        work / "archives",
        "--batch-id",
        path.stem,
        "--collected-at",
        STAMP,
        "--format",
        "fixture",
    )
    print("INPUT", folder, (Path(folder) / "manifest.json").read_text(), flush=True)
    if pause:
        # This group has five outstanding records before the 1,000-line batch.
        limited = stream(
            work, "backpressure", "publish", TOPIC, folder, "--max-lag", "37", code=1
        )
        check(
            "publisher stopped at its last acknowledged line",
            json.loads(limited[-1])["acknowledged"],
            32,
        )
    sent = stream(work, "publish-" + path.stem, "publish", TOPIC, folder)
    if pause:
        check(
            "publisher resumes remaining lines after checkpoint",
            json.loads(sent[-1])["published"],
            968,
        )
    return Path(folder)


def setup(work):
    work.mkdir(parents=True, exist_ok=False)
    stream(work, "init", "init", TOPIC)
    rows = (FIXTURES / "cases.jsonl").read_bytes().splitlines(keepends=True)
    (work / "seed.jsonl").write_bytes(rows[0])
    publish_file(work, work / "seed.jsonl")
    projects = {}
    for scenario in SCENARIOS:
        command(
            work, "migrate-" + scenario, "alembic", "upgrade", "head", scenario=scenario
        )
        drain(work, "seed-" + scenario, scenario)
        db = engine(scenario)
        with db.connect() as conn:
            event_id = conn.scalar(
                text(
                    "SELECT event_id FROM current_revisions WHERE paper_id='fixture:river'"
                )
            )
        project = create_project(db, "River review", "count streams")
        save_decision(
            db,
            project,
            "fixture:river",
            event_id,
            "relevant",
            "two streams reported",
            str(uuid4()),
        )
        projects[scenario] = project
        db.dispose()
    (work / "projects.json").write_text(json.dumps(projects))
    with closing(
        KafkaProducer(bootstrap_servers=os.environ["KAFKA_BOOTSTRAP"], acks="all")
    ) as producer:
        ack = producer.send(TOPIC, key=b"fixture:river", value=BAD).get(timeout=15)
    check("malformed record follows seed on river partition", ack.offset, 1)
    (work / "partition.txt").write_text(str(ack.partition))
    # The changed abstract at offset 3 has a healthy stale event behind it in the same fetch.
    (work / "updates.jsonl").write_bytes(b"".join((rows[3], rows[4], rows[1])))
    updates = publish_file(work, work / "updates.jsonl")
    resume = stream(work, "resume-acked", "publish", TOPIC, updates)
    check(
        "acknowledged archive resume sends zero", json.loads(resume[-1])["published"], 0
    )
    for scenario, boundary in (("before", "before-commit"), ("after", "after-commit")):
        where = f"{ack.partition}:3"
        records = drain(
            work,
            "crash-" + scenario,
            scenario,
            fault=True,
            code=86,
            extra=("--test-fault", boundary, "--test-at", where),
        )
        check(
            "process died at selected SQL boundary", records[-1]["boundary"], boundary
        )
        part = status(work, "stopped-" + scenario, scenario)[str(ack.partition)]
        check(
            scenario + " next offset and broker end",
            [part["committed"], part["end"]],
            [3, 5],
        )
        with engine(scenario).connect() as conn:
            counts = [
                conn.scalar(text("SELECT COUNT(*) FROM " + t))
                for t in (
                    "processed_events",
                    "paper_revisions",
                    "derived_jobs",
                    "rejected_events",
                )
            ]
            version = conn.scalar(text("SELECT MAX(version_rank) FROM paper_revisions"))
        check(
            scenario + " markers, revisions, jobs, quarantined, maximum version",
            [*counts, version],
            [2, 2, 1, 1, 2] if scenario == "before" else [3, 3, 2, 1, 3],
        )


def outage(work):
    old = status(work, "outage-start", "before")
    records = drain(work, "database-offline", "before", code=1)
    failures = [r for r in records if r["kind"] == "sql_error"]
    check(
        "bounded retries of the same unfinished offset",
        [(r["attempt"], r["offset"]) for r in failures],
        [(1, 3), (2, 3), (3, 3), (4, 3)],
    )
    check(
        "all broker offsets unchanged throughout stopped MySQL",
        status(work, "outage-end", "before"),
        old,
    )


def snapshot(work, scenario):
    db = engine(scenario)
    project = json.loads((work / "projects.json").read_text())[scenario]
    decision = read_decision(db, project, "fixture:river")
    rows = export_rows(db)
    generation = next(rows)["generation"]
    papers = {r["paper_id"]: r for r in rows}
    with db.connect() as conn:
        revisions = conn.scalar(text("SELECT COUNT(*) FROM paper_revisions"))
        marker_rows = conn.execute(
            text("SELECT event_id,outcome FROM processed_events ORDER BY event_id")
        ).all()
        rejections = dict(
            conn.execute(text("SELECT raw_sha256,reason FROM rejected_events")).all()
        )
        jobs = conn.execute(
            text(
                "SELECT paper_id,text_hash,kind,model_id,model_revision,preprocessing,status "
                "FROM derived_jobs ORDER BY paper_id,text_hash"
            )
        ).all()
        history = conn.scalar(text("SELECT COUNT(*) FROM screening_history"))
    db.dispose()
    check(
        scenario + " canonical records, revisions, markers",
        [len(papers), revisions, len(marker_rows)],
        [986, 993, 993],
    )
    check(
        scenario + " offline oracle digest",
        digest(papers),
        EXPECTED["development"]["state_sha256"],
    )
    check(
        scenario + " quarantine equality",
        rejections
        == {**EXPECTED["rejections"], hashlib.sha256(BAD).hexdigest(): "invalid_json"},
        True,
    )
    check(
        "wire bytes archived",
        (work / "quarantine" / hashlib.sha256(BAD).hexdigest()).read_bytes(),
        BAD,
    )
    decision_fields = [
        decision[k]
        for k in (
            "label",
            "note",
            "reviewed_event_id",
            "reviewed_text_hash",
            "current_text_hash",
            "needs_reconsideration",
        )
    ]
    check(
        "preserved label and history",
        [decision_fields[0], decision_fields[1], decision_fields[-1], history],
        ["relevant", "two streams reported", True, 1],
    )
    return dict(
        corpus=digest(papers),
        generation=generation,
        decision=decision_fields,
        markers=digest([list(row) for row in marker_rows]),
        rejected=digest(rejections),
        jobs=digest([list(row) for row in jobs]),
        job_count=len(jobs),
    )


def recover(work):
    command(
        work,
        "generate",
        sys.executable,
        FIXTURES / "generate.py",
        work / "development.jsonl",
    )
    folder = publish_file(work, work / "development.jsonl", pause=True)
    check(
        "development archive SHA-256",
        json.loads((folder / "manifest.json").read_text())["sha256"],
        EXPECTED["development"]["input_sha256"],
    )
    baseline = None
    partition = int((work / "partition.txt").read_text())
    for scenario in SCENARIOS:
        records = drain(work, "recover-" + scenario, scenario)
        target = [
            r["outcome"]
            for r in records
            if r["kind"] == "offset_commit"
            and r["partition"] == partition
            and r["offset"] == 3
        ]
        check(
            scenario + " result for interrupted record",
            target,
            ["duplicate" if scenario == "after" else "applied"],
        )
        check(
            "caught up on every partition",
            [p["lag"] for p in records[-1]["partitions"].values()],
            [0, 0, 0],
        )
        actual = snapshot(work, scenario)
        if baseline is None:
            baseline = actual
            print("Uninterrupted durable state", json.dumps(actual, sort_keys=True))
        check(
            scenario
            + " matches uninterrupted corpus, label, ledger, jobs and generation",
            actual == baseline,
            True,
        )
    # Re-read the whole retained topic with a brand-new group into the recovered database.
    fresh = drain(work, "fresh-group", "after", group="fresh")
    check(
        "fresh group outcomes", fresh[-1]["outcomes"], {"duplicate": 999, "rejected": 6}
    )
    check(
        "fresh group preserves every durable effect", snapshot(work, "after"), baseline
    )
    stream(work, "replay-publication", "publish", TOPIC, folder, "--replay")
    again = drain(work, "replayed-batch", "after", group="fresh")
    check(
        "republished batch outcomes",
        again[-1]["outcomes"],
        {"duplicate": 995, "rejected": 5},
    )
    check(
        "republication preserves every durable effect",
        snapshot(work, "after"),
        baseline,
    )


if __name__ == "__main__":
    stage, directory = sys.argv[1:]
    {"setup": setup, "outage": outage, "recover": recover}[stage](Path(directory))
