# AI-generated tests (Codex): run on a fresh MySQL volume before and after a service restart.
import hashlib
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from threading import Barrier
from types import SimpleNamespace
from uuid import uuid4

from sqlalchemy import create_engine, event as sql_event, text
from sqlalchemy.exc import DBAPIError

from papersearch.adapters import json_record, parse_record, raw_lines
from papersearch.config import mysql_url
from papersearch.contracts import digest
from papersearch.ingestion import apply_delivery_batch
from papersearch.storage.jobs import claim_job, finish_job
from papersearch.storage.metadata import apply_event, export_rows
from papersearch.storage.reviews import read_decision, save_decision

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "phase1"
EXPECTED = json.loads((FIXTURE / "expected.json").read_text())
STAMP = "2020-02-01T00:00:00Z"
ENGINE = create_engine(mysql_url(dict(os.environ)))
RIVER = "fixture:river"


def check(name, actual, expected):
    assert actual == expected, f"{name}: expected {expected!r}, got {actual!r}"
    print(f"PASS {name}: expected={expected!r} actual={actual!r}", flush=True)


def query(sql):
    with ENGINE.connect() as conn:
        return conn.execute(text(sql)).all()


def cli(*args):
    output = subprocess.check_output(["papersearch", "db", *map(str, args)], text=True)
    return json.loads(output)


def race(operation, count=4):
    barrier = Barrier(count)

    def run(_):
        barrier.wait(timeout=10)
        return operation()

    with ThreadPoolExecutor(count) as pool:
        return list(pool.map(run, range(count)))


def generation():
    return query("SELECT generation FROM system_state")[0][0]


def state():
    records = iter(export_rows(ENGINE, page_size=17))
    header = next(records)
    papers = {r["paper_id"]: r for r in records}
    return [header["generation"], len(papers), digest(papers)]


def current_state_checks():
    gen, count, checksum = state()
    check(
        "canonical count and SHA-256",
        [count, checksum],
        [
            EXPECTED["development"]["current_count"],
            EXPECTED["development"]["state_sha256"],
        ],
    )
    for table in ("processed_events", "paper_revisions"):
        check(
            table,
            query(f"SELECT COUNT(*) FROM {table}")[0][0],
            EXPECTED["development"]["unique_events"],
        )
    check(
        "conflicting event identities",
        sorted(
            r[0]
            for r in query(
                "SELECT event_id FROM processed_events WHERE outcome='conflict'"
            )
        ),
        sorted(i for group in EXPECTED["conflicts"] for i in group["event_ids"]),
    )
    check(
        "rejection identities and reasons",
        dict(query("SELECT raw_sha256,reason FROM rejected_events")),
        EXPECTED["rejections"],
    )
    return gen


def job_digest():
    rows = query("SELECT * FROM derived_jobs ORDER BY job_id")
    return digest([[str(v) for v in row] for row in rows])


def before(work):
    fixture = work / "development.jsonl"
    subprocess.run([sys.executable, FIXTURE / "generate.py", fixture], check=True)
    check(
        "fixture SHA-256",
        hashlib.sha256(fixture.read_bytes()).hexdigest(),
        EXPECTED["development"]["input_sha256"],
    )
    events = []
    for line, checksum, raw in raw_lines(FIXTURE / "cases.jsonl"):
        if line > 5:
            break
        events.append(
            parse_record(
                json_record(raw),
                "fixture",
                batch_id="m2",
                collected_at=STAMP,
                checksum=checksum,
                location=f"{FIXTURE}/cases.jsonl#{line}",
            )
        )
    first, stale, _, authors, changed = events
    old_id = first.canonical()["event_id"]
    new_id = changed.canonical()["event_id"]

    def inject(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("INSERT INTO processed_events"):
            return "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='M2 injected rollback'", ()
        return statement, parameters

    sql_event.listen(ENGINE, "before_cursor_execute", inject, retval=True)
    try:
        apply_event(ENGINE, first)
        raise AssertionError("database fault was not raised")
    except DBAPIError as exc:
        assert "M2 injected rollback" in str(exc.orig)
        print("Observed MySQL transaction failure:", exc.orig)
    finally:
        sql_event.remove(ENGINE, "before_cursor_execute", inject)
    tables = [
        "papers",
        "paper_revisions",
        "current_revisions",
        "processed_events",
        "derived_jobs",
    ]
    check(
        "rollback counts for " + ", ".join(tables),
        [query(f"SELECT COUNT(*) FROM {t}")[0][0] for t in tables],
        [0] * len(tables),
    )
    check("rolled-back generation", generation(), 0)
    check(
        "four concurrent deliveries",
        sorted(race(lambda: apply_event(ENGINE, first))),
        ["applied", "duplicate", "duplicate", "duplicate"],
    )
    check(
        "single committed effect",
        [query(f"SELECT COUNT(*) FROM {t}")[0][0] for t in tables],
        [1] * 5,
    )
    check(
        "older source version, later source time", apply_event(ENGINE, stale), "stale"
    )
    check("generation after stale event", generation(), 1)
    leases = [job for job in race(lambda: claim_job(ENGINE)) if job]
    check("owners from four claimers for one job", len(leases), 1)
    job = leases[0]
    check(
        "retryable failure releases lease",
        finish_job(
            ENGINE, job["job_id"], job["lease_token"], error="temporary", retryable=True
        ),
        True,
    )
    second = claim_job(ENGINE)
    check(
        "released token fenced",
        finish_job(ENGINE, job["job_id"], job["lease_token"]),
        False,
    )
    check("retry attempt", second["attempts"], 2)
    check(
        "successful current owner",
        finish_job(ENGINE, second["job_id"], second["lease_token"]),
        True,
    )

    a, b = cli("project", "River A", "--criteria", "flow"), cli("project", "River B")
    request = str(uuid4())
    first_history = cli(
        "decide",
        a,
        RIVER,
        old_id,
        "uncertain",
        "--note",
        "check methods",
        "--request-id",
        request,
    )
    check("author update", apply_event(ENGINE, authors), "applied")
    check(
        "unchanged text keeps review current",
        read_decision(ENGINE, a, RIVER)["needs_reconsideration"],
        False,
    )
    check(
        "unchanged text reuses job", query("SELECT COUNT(*) FROM derived_jobs")[0][0], 1
    )
    stream = export_rows(ENGINE, page_size=1)
    check("snapshot opened at generation", next(stream), {"generation": 2})
    barrier = Barrier(2)

    def save_while_revising():
        barrier.wait(timeout=10)
        return save_decision(
            ENGINE, b, RIVER, old_id, "irrelevant", "wrong scope", str(uuid4())
        )

    with ThreadPoolExecutor(1) as pool:
        review = pool.submit(save_while_revising)
        barrier.wait(timeout=10)
        check(
            "new abstract during screening save",
            apply_event(ENGINE, changed),
            "applied",
        )
        assert review.result() > 0
    check(
        "open export retains earlier revision",
        [r["event_id"] for r in stream],
        [authors.canonical()["event_id"]],
    )
    check("fresh export sees next generation", generation(), 3)
    check(
        "new text creates second job",
        query("SELECT COUNT(*) FROM derived_jobs")[0][0],
        2,
    )
    check(
        "both previous reviews flagged",
        [read_decision(ENGINE, p, RIVER)["needs_reconsideration"] for p in (a, b)],
        [True, True],
    )
    edit = str(uuid4())
    ids = race(
        lambda: save_decision(ENGINE, a, RIVER, new_id, "relevant", "re-read", edit)
    )
    check("four concurrent saves append one history row", len(set(ids)), 1)
    check(
        "old request retry returns original history",
        save_decision(ENGINE, a, RIVER, old_id, "uncertain", "check methods", request),
        first_history,
    )
    try:
        save_decision(ENGINE, a, RIVER, old_id, "relevant", "different", request)
        raise AssertionError("request reuse was accepted")
    except ValueError as exc:
        check(
            "conflicting retry rejected",
            str(exc),
            "Decision request ID was reused with different content",
        )

    args = (
        "apply",
        fixture,
        "--format",
        "fixture",
        "--batch-id",
        "m2",
        "--collected-at",
        STAMP,
    )
    print("Initial full-file delivery after targeted events:", cli(*args))
    gen = current_state_checks()
    check("full fixture replay", cli(*args), {"duplicate": 995, "rejected": 5})
    check("replay preserves generation", generation(), gen)
    # Leave the oldest unfinished job leased. The second process compares its exact
    # state first and then tries both the old and new tokens after database restart.
    abandoned = claim_job(ENGINE, lease_seconds=1)
    decisions = [cli("decision", p, RIVER) for p in (a, b)]
    check(
        "project label, note and flag",
        [[d["label"], d["note"], d["needs_reconsideration"]] for d in decisions],
        [["relevant", "re-read", False], ["irrelevant", "wrong scope", True]],
    )
    check(
        "append-only history", query("SELECT count(*) FROM screening_history")[0][0], 3
    )
    proof = dict(
        projects=[a, b],
        decisions=decisions,
        generation=gen,
        jobs=job_digest(),
        abandoned=abandoned,
        uuid=query("SELECT @@server_uuid")[0][0],
    )
    (work / "before.json").write_text(json.dumps(proof))
    print(
        f"Before restart: generation={gen} server={proof['uuid']} jobs={proof['jobs']}"
    )


def after(work):
    proof = json.loads((work / "before.json").read_text())
    check(
        "persistent MySQL server UUID",
        query("SELECT @@server_uuid")[0][0],
        proof["uuid"],
    )
    check("generation persisted", current_state_checks(), proof["generation"])
    check("every queued job and lease persisted", job_digest(), proof["jobs"])
    check(
        "both decision records persisted",
        [cli("decision", p, RIVER) for p in proof["projects"]] == proof["decisions"],
        True,
    )
    check(
        "all history revisions persisted",
        query("SELECT COUNT(*) FROM screening_history")[0][0],
        3,
    )
    job = proof["abandoned"]
    check(
        "expired owner refused before reclaim",
        finish_job(ENGINE, job["job_id"], job["lease_token"]),
        False,
    )
    recovered = claim_job(ENGINE)
    check(
        "abandoned job reclaimed",
        [recovered["job_id"], recovered["attempts"]],
        [job["job_id"], 2],
    )
    check(
        "expired owner refused after reclaim",
        finish_job(ENGINE, job["job_id"], job["lease_token"]),
        False,
    )
    check(
        "new owner completes",
        finish_job(ENGINE, job["job_id"], recovered["lease_token"]),
        True,
    )
    check(
        "old owner cannot overwrite completion",
        finish_job(ENGINE, job["job_id"], job["lease_token"], error="late"),
        False,
    )
    for attempt in (1, 2, 3):
        retry = claim_job(ENGINE)
        check("bounded retry attempt", retry["attempts"], attempt)
        check(
            "worker records error",
            finish_job(
                ENGINE,
                retry["job_id"],
                retry["lease_token"],
                error="dependency unavailable",
            ),
            True,
        )
    check(
        "terminal attempt and retained reason",
        tuple(
            query(
                "SELECT status,attempts,last_error FROM derived_jobs WHERE status='failed'"
            )[0]
        ),
        ("failed", 3, "dependency unavailable"),
    )
    before_jobs = query("SELECT COUNT(*) FROM derived_jobs")[0][0]
    check(
        "out-of-collection arXiv metadata imports",
        cli(
            "apply",
            FIXTURE / "arxiv-metadata.jsonl",
            "--format",
            "arxiv",
            "--batch-id",
            "m2-arxiv",
            "--collected-at",
            STAMP,
        ),
        {"applied": 1},
    )
    check(
        "out-of-collection import creates no embedding backlog",
        query("SELECT COUNT(*) FROM derived_jobs")[0][0],
        before_jobs,
    )
    batch_delivery_checks(work)


def batch_delivery_checks(work):
    line, checksum, raw = next(raw_lines(FIXTURE / "cases.jsonl"))
    template = parse_record(
        json_record(raw),
        "fixture",
        batch_id="m8",
        collected_at=STAMP,
        checksum=checksum,
        location=f"fixture#{line}",
    )

    def wire(key):
        item = replace(
            template,
            paper_id="fixture:bulk-" + key,
            payload=replace(
                template.payload,
                abstract="We compare delimiter selection methods in multilingual text.",
            ),
        )
        return SimpleNamespace(
            key=item.paper_id.encode(), value=json.dumps(item.envelope()).encode()
        )

    def deliver(*rows):
        return dict(apply_delivery_batch(ENGINE, rows, work / "quarantine"))

    rows = [wire(k) for k in ("a", "b", "c")]

    def interrupt(_):
        raise RuntimeError("controlled batch interruption")

    sql_event.listen(ENGINE, "commit", interrupt, once=True)
    try:
        deliver(*rows)
    except RuntimeError as exc:
        assert str(exc) == "controlled batch interruption"
    else:
        raise AssertionError("Expected interrupted transaction")
    check(
        "batch interruption rolls back every parent",
        query("SELECT COUNT(*) FROM papers WHERE paper_id LIKE 'fixture:bulk-%'")[0][0],
        0,
    )
    outcomes = race(lambda: deliver(*rows), 2)
    check(
        "overlapping first deliveries resolve exactly once",
        sorted(tuple(sorted(r.items())) for r in outcomes),
        [(("applied", 3),), (("duplicate", 3),)],
    )
    check(
        "mixed new, replay and malformed delivery",
        deliver(wire("d"), rows[1], SimpleNamespace(key=b"bad", value=None), wire("e")),
        {"applied": 2, "duplicate": 1, "rejected": 1},
    )
    check(
        "bulk revision and marker counts",
        tuple(
            query("""
        SELECT COUNT(*), COUNT(p.event_id) FROM paper_revisions r
        LEFT JOIN processed_events p ON r.event_id=p.event_id WHERE r.paper_id LIKE 'fixture:bulk-%'
        """)[0]
        ),
        (5, 5),
    )


if __name__ == "__main__":
    stage, directory = sys.argv[1:]
    work = Path(directory)
    if stage == "before":
        work.mkdir(parents=True, exist_ok=False)
        before(work)
    elif stage == "after":
        after(work)
    else:
        raise ValueError(stage)
