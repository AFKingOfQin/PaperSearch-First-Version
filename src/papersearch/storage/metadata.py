# AI-generated code (Codex): transactional revision selection and consistent corpus reads.
import json
from collections import Counter
from pathlib import Path

from mysql.connector import Error as MySQLError
from mysql.connector.connection import MySQLConnection
from mysql.connector.cursor import MySQLCursorPrepared
from sqlalchemy import bindparam, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.engine import Engine

from ..adapters import json_record, parse_record, raw_lines
from ..encoder import eligibility_sql
from ..contracts import Event, RecordError, provenance, utc


def revision_args(event: Event):
    record = event.canonical()
    return dict(
        paper=event.paper_id,
        event=record["event_id"],
        rank=event.source_version or 0,
        stamp=event.source_updated_at,
        hash=record["embedding_text_hash"],
        record=json.dumps(record, ensure_ascii=False),
        batch=event.batch_id,
        collected=event.collected_at,
        location=event.raw_location,
        raw=event.raw_sha256,
    )


REVISION_FIELDS = (
    "paper",
    "event",
    "rank",
    "stamp",
    "hash",
    "record",
    "batch",
    "collected",
    "location",
    "raw",
)
INSERT_REVISION = """
    INSERT INTO paper_revisions
        (paper_id,event_id,version_rank,source_updated_at,text_hash,record_json,
         batch_id,collected_at,raw_location,raw_sha256)
    VALUES """


def insert_revisions(conn, rows):
    # Use the binary parameter protocol: source prose may contain MySQL client script delimiters.
    placeholders = "(" + ",".join(["%s"] * len(REVISION_FIELDS)) + ")"
    statement = INSERT_REVISION + ",".join([placeholders] * len(rows))
    values = tuple(row[key] for row in rows for key in REVISION_FIELDS)
    driver = conn.connection.driver_connection
    if isinstance(driver, MySQLConnection):
        cursor_type = MySQLCursorPrepared
    else:
        # The C extension inherits the connection's buffered flag even when passed False.
        from mysql.connector.cursor_cext import CMySQLCursorPrepared
        cursor_type = CMySQLCursorPrepared
    try:
        with driver.cursor(prepared=True, cursor_class=cursor_type) as cursor:
            cursor.execute(statement, values)
    except MySQLError as exc:
        disconnected = conn.dialect.is_disconnect(exc, driver, None)
        if disconnected:
            conn.invalidate(exc)
        raise DBAPIError.instance(
            statement, None, exc, MySQLError, connection_invalidated=disconnected
        ) from exc


def apply_fresh_events(engine: Engine, events: list[Event]) -> bool:
    """Atomically apply a bounded group of first-seen papers. A collision rolls back the entire group."""
    if not 1 <= len(events) <= 256:
        raise ValueError("Expected 1 to 256 normalized events")
    rows = sorted((revision_args(e) for e in events), key=lambda r: r["paper"])
    ids = [r["paper"] for r in rows]
    if len(set(ids)) != len(ids):
        return False
    with engine.begin() as conn:
        # Do not ignore a collision, including one with a concurrently inserted parent.
        try:
            conn.execute(text("INSERT INTO papers (paper_id) VALUES (:paper)"), rows)
        except IntegrityError as error:
            if getattr(error.orig, "errno", None) != 1062:
                raise
            conn.rollback()
            return False
        insert_revisions(conn, rows)
        conn.execute(
            text(
                "INSERT INTO current_revisions (paper_id,event_id) VALUES (:paper,:event)"
            ),
            rows,
        )
        conn.execute(
            text(
                "INSERT INTO processed_events (event_id,outcome) VALUES (:event,'applied')"
            ),
            rows,
        )
        conn.execute(
            text(f"""
            INSERT INTO derived_jobs(paper_id,text_hash,kind,model_id,model_revision,preprocessing)
            SELECT r.paper_id,r.text_hash,s.kind,s.model_id,s.model_revision,s.preprocessing
            FROM paper_revisions r JOIN derivation_state s ON s.singleton=1
            WHERE r.paper_id IN :ids AND {eligibility_sql("r.paper_id", "s.kind")}
        """).bindparams(bindparam("ids", expanding=True)),
            dict(ids=ids),
        )
        conn.execute(
            text("UPDATE system_state SET generation=generation+:n WHERE singleton=1"),
            dict(n=len(rows)),
        )
    return True


def apply_event(engine: Engine, event: Event) -> str:
    args = revision_args(event)
    with engine.begin() as conn:
        # Acquire the stable parent lock before the transaction's first snapshot read.
        conn.execute(
            text(
                "INSERT INTO papers VALUES (:paper) ON DUPLICATE KEY UPDATE paper_id=:paper"
            ),
            args,
        )
        if conn.scalar(
            text("SELECT outcome FROM processed_events WHERE event_id=:event"), args
        ):
            return "duplicate"
        old = conn.scalar(
            text("SELECT event_id FROM current_revisions WHERE paper_id=:paper"), args
        )
        insert_revisions(conn, [args])
        siblings = conn.execute(
            text("""
            SELECT event_id FROM paper_revisions WHERE paper_id=:paper AND version_rank=:rank
              AND source_updated_at=:stamp AND event_id<>:event LIMIT 1
        """),
            args,
        ).first()
        if siblings:
            conn.execute(
                text("""
                UPDATE processed_events p JOIN paper_revisions r ON r.event_id=p.event_id
                SET p.outcome='conflict' WHERE r.paper_id=:paper AND r.version_rank=:rank
                AND r.source_updated_at=:stamp
            """),
                args,
            )
        winner = conn.scalar(
            text("""
            SELECT MIN(event_id) FROM paper_revisions WHERE paper_id=:paper
            GROUP BY version_rank,source_updated_at HAVING COUNT(*)=1
            ORDER BY version_rank DESC,source_updated_at DESC LIMIT 1
        """),
            args,
        )
        if winner != old:
            if winner is None:
                conn.execute(
                    text("DELETE FROM current_revisions WHERE paper_id=:paper"), args
                )
            else:
                args.update(winner=winner)
                conn.execute(
                    text("""
                    INSERT INTO current_revisions VALUES (:paper,:winner)
                    ON DUPLICATE KEY UPDATE event_id=:winner
                """),
                    args,
                )
                conn.execute(
                    text(f"""
                    INSERT INTO derived_jobs (paper_id,text_hash,kind,model_id,model_revision,preprocessing)
                    SELECT :paper,r.text_hash,s.kind,s.model_id,s.model_revision,s.preprocessing
                    FROM paper_revisions r JOIN derivation_state s ON s.singleton=1
                    WHERE r.event_id=:winner AND {eligibility_sql('r.paper_id', 's.kind')}
                    ON DUPLICATE KEY UPDATE job_id=derived_jobs.job_id
                """),
                    args,
                )
            conn.execute(
                text(
                    "UPDATE system_state SET generation=generation+1 WHERE singleton=1"
                )
            )
        outcome = (
            "conflict"
            if siblings
            else ("applied" if winner == args["event"] else "stale")
        )
        conn.execute(
            text(
                "INSERT INTO processed_events (event_id,outcome) VALUES (:event,:outcome)"
            ),
            {**args, "outcome": outcome},
        )
    return outcome


def reject_record(engine: Engine, checksum: str, fmt: str, reason: str, location: str):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO rejected_events (raw_sha256,input_format,reason,raw_location)
            VALUES (:raw,:fmt,:reason,:location) ON DUPLICATE KEY UPDATE raw_sha256=:raw
        """), dict(raw=checksum, fmt=fmt, reason=reason, location=location))


def apply_file(
    engine: Engine, path: Path, fmt: str, batch_id=None, collected_at=None
) -> dict:
    if fmt != "event":
        batch_id, collected_at = provenance(batch_id, 256), utc(collected_at)
    counts = Counter()
    for line, checksum, raw in raw_lines(path):
        location = f"{path.as_posix()}#{line}"
        try:
            event = parse_record(
                json_record(raw),
                fmt,
                batch_id=batch_id,
                collected_at=collected_at,
                location=location,
                checksum=checksum,
            )
        except RecordError as exc:
            reject_record(engine, checksum, fmt, str(exc), location)
            counts["rejected"] += 1
        else:
            counts[apply_event(engine, event)] += 1
    return dict(counts)


def export_rows(engine: Engine, page_size: int = 128):
    """Stream a generation header followed by its canonical records, ordered by paper ID."""
    if not 1 <= page_size <= 1000:
        raise ValueError("page_size must be between 1 and 1000")
    with (
        engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn,
        conn.begin(),
    ):
        yield {
            "generation": conn.scalar(
                text("SELECT generation FROM system_state WHERE singleton=1")
            )
        }
        after = ""
        while True:
            page = current_page(conn, after, page_size)
            for after, record in page:
                yield json.loads(record)
            if len(page) < page_size:
                break

def current_page(conn, after, page_size, recipe=None):
    """Read from the caller's corpus snapshot, optionally including matching cached bytes."""
    columns = join = ""
    args = {"after": after, "limit": page_size}
    if recipe is not None:
        args.update(recipe)
        columns = ",a.cache_key,a.shard_sha,a.slot,a.vector_sha"
        join = f"""LEFT JOIN derived_artifacts a ON a.text_hash=r.text_hash AND a.kind=:kind
                  AND a.model_id=:model_id AND a.model_revision=:model_revision
                  AND a.preprocessing=:preprocessing AND {eligibility_sql('c.paper_id', ':kind')}"""
    # Limit IDs before joining large JSON revisions. A hash join can otherwise sort the remaining corpus per page.
    return conn.execute(text(f"""
        SELECT c.paper_id,r.record_json {columns}
        FROM (SELECT paper_id,event_id FROM current_revisions
              WHERE paper_id>:after ORDER BY paper_id LIMIT :limit) c
        JOIN paper_revisions r ON r.event_id=c.event_id {join}
        ORDER BY c.paper_id
    """), args).all()
