# AI-generated code (Codex): local HTTP endpoints for lexical screening and durable reviews.
import csv
import io
import json
import os
from contextlib import closing
from pathlib import Path
from typing import Annotated, Literal
from uuid import UUID

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
from kafka.errors import KafkaError
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from .broker import group_reader, offsets
from .config import mysql_url
from .lexical import LexicalReader
from .snapshots import STATE, published_manifest, status
from .storage.reviews import create_project, decision_on_connection, save_decision

app = FastAPI(title="PaperSearch")
engine = create_engine(mysql_url(os.environ), pool_pre_ping=True)
root = Path(os.environ.get("SNAPSHOT_ROOT", "/artifacts/derived"))
reader = LexicalReader(root)

Paper = Annotated[str, Query(min_length=1, max_length=128)]
Event = Annotated[str, Query(pattern="^[a-f0-9]{64}$")]
Label = Literal["relevant", "irrelevant", "uncertain"]


class Project(BaseModel):
    topic: str = Field(min_length=1, max_length=1024)
    criteria: str = Field(default="", max_length=8000)


class Decision(BaseModel):
    reviewed_event_id: str = Field(pattern="^[a-f0-9]{64}$")
    label: Label
    note: str = Field(default="", max_length=8000)
    request_id: UUID


def check_project(conn, project):
    if not conn.scalar(
        text("SELECT 1 FROM review_projects WHERE project_id=:id"), {"id": str(project)}
    ):
        raise HTTPException(404, "Unknown project")


def snapshot_status():
    try:
        return status(engine, root)
    except (ValueError, OSError, SQLAlchemyError) as exc:
        raise HTTPException(503, "Snapshot status unavailable") from exc


@app.get("/health")
def health():
    return {"service": "papersearch"}


@app.get("/ready")
def readiness():
    manifest = snapshot_status()["active"]
    if not manifest:
        raise HTTPException(503, "Publish a snapshot to enable search")
    try:
        reader.path(manifest)
    except (ValueError, OSError) as exc:
        raise HTTPException(503, "Published index unavailable") from exc
    return {"snapshot_id": manifest["snapshot_id"]}


@app.get("/status")
def processing():
    result = snapshot_status()
    with engine.connect() as conn:
        result["input"] = {
            table: conn.scalar(text("SELECT COUNT(*) FROM " + table))
            for table in ("processed_events", "rejected_events", "current_revisions")
        }
    topic = os.environ.get("PAPERSEARCH_TOPIC", "papers")
    try:
        with closing(
            group_reader(
                os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
                os.environ.get("PAPERSEARCH_GROUP", "papersearch-ingest"),
            )
        ) as consumer:
            partitions = offsets(consumer, topic)
        result["broker"] = dict(
            topic=topic,
            partitions=partitions,
            lag=sum(p["lag"] for p in partitions.values()),
        )
    except (KafkaError, ValueError) as exc:
        result["broker"] = dict(topic=topic, lag=None, error=type(exc).__name__)
    return result


@app.post("/projects", status_code=201)
def new_project(body: Project):
    if not body.topic.strip():
        raise HTTPException(422, "Enter a topic")
    return {"project_id": create_project(engine, body.topic.strip(), body.criteria)}


@app.get("/projects")
def projects():
    with engine.connect() as conn:
        return list(
            conn.execute(
                text(
                    "SELECT project_id,topic,criteria FROM review_projects ORDER BY topic,project_id"
                )
            ).mappings()
        )


@app.get("/projects/{project}/search")
def search(
    project: UUID,
    q: Annotated[str, Query(min_length=1, max_length=512)],
    ranker: Literal["bm25"] = "bm25",
    field: Literal["all", "title", "abstract"] = "all",
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    snapshot: str | None = None,
):
    with engine.connect() as conn:
        check_project(conn, project)
        state = conn.execute(text(STATE)).mappings().one()
        try:
            active = published_manifest(conn, root, state["active_snapshot"])
        except (OSError, ValueError) as exc:
            raise HTTPException(503, "Published manifest unavailable") from exc
    if not active:
        raise HTTPException(503, "No published snapshot")
    if snapshot is not None and snapshot != active["snapshot_id"]:
        raise HTTPException(409, "A new snapshot is active. Start the search again.")
    try:
        hits, more = reader.search(active, q, field, offset, limit)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    except OSError as exc:
        raise HTTPException(503, "Published index unavailable") from exc
    return dict(
        results=hits,
        has_more=more,
        snapshot_id=active["snapshot_id"],
        ranker=ranker,
        generation=active["generation"],
        metadata_generations_behind=state["generation"] - active["generation"],
    )


@app.get("/papers/detail")
def detail(paper: Paper, event: Event | None = None):
    with engine.connect() as conn:
        args = dict(paper=paper, event=event)
        current = conn.scalar(
            text("SELECT event_id FROM current_revisions WHERE paper_id=:paper"), args
        )
        args["event"] = event or current
        record = conn.scalar(
            text(
                "SELECT record_json FROM paper_revisions WHERE paper_id=:paper AND event_id=:event"
            ),
            args,
        )
        if record is None:
            raise HTTPException(404, "Unknown paper or revision")
        revisions = conn.execute(
            text(
                "SELECT event_id,version_rank,source_updated_at,text_hash FROM paper_revisions "
                "WHERE paper_id=:paper ORDER BY version_rank DESC,source_updated_at DESC,event_id"
            ),
            args,
        )
        return dict(
            record=json.loads(record),
            current_event_id=current,
            revisions=list(revisions.mappings()),
        )


@app.put("/projects/{project}/decision")
def decide(project: UUID, paper: Paper, body: Decision):
    try:
        history = save_decision(
            engine,
            str(project),
            paper,
            body.reviewed_event_id,
            body.label,
            body.note,
            str(body.request_id),
        )
    except ValueError as exc:
        code = 404 if str(exc).startswith("Unknown") else 409
        raise HTTPException(code, str(exc)) from exc
    return {"history_id": history}


@app.get("/projects/{project}/decision")
def decision(project: UUID, paper: Paper):
    args = dict(project=str(project), paper=paper)
    with engine.connect() as conn:
        check_project(conn, project)
        if not conn.scalar(text("SELECT 1 FROM papers WHERE paper_id=:paper"), args):
            raise HTTPException(404, "Unknown paper")
        history = list(
            conn.execute(
                text(
                    "SELECT history_id,label,note,reviewed_event_id,saved_at FROM screening_history "
                    "WHERE project_id=:project AND paper_id=:paper ORDER BY history_id"
                ),
                args,
            ).mappings()
        )
        # Both reads use this transaction's consistent view during concurrent edits.
        current = decision_on_connection(conn, str(project), paper)
    return dict(current=current, history=history)


EXPORT_FIELDS = (
    "project_id",
    "paper_id",
    "label",
    "note",
    "reviewed_event_id",
    "reviewed_text_hash",
    "current_text_hash",
    "needs_reconsideration",
    "saved_at",
)


@app.get("/projects/{project}/export.csv")
def export(project: UUID):
    with engine.connect() as conn:
        check_project(conn, project)

    def rows():
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(EXPORT_FIELDS)
        yield buffer.getvalue()
        with engine.connect().execution_options(
            isolation_level="REPEATABLE READ"
        ) as conn:
            after = ""
            while True:
                page = conn.execute(
                    text("""
                    SELECT d.project_id,d.paper_id,h.label,h.note,h.reviewed_event_id,r.text_hash,
                           v.text_hash,NOT (r.text_hash <=> v.text_hash),h.saved_at
                    FROM screening_decisions d JOIN screening_history h ON h.history_id=d.history_id
                    JOIN paper_revisions r ON r.event_id=h.reviewed_event_id
                    LEFT JOIN current_revisions c ON c.paper_id=d.paper_id
                    LEFT JOIN paper_revisions v ON v.event_id=c.event_id
                    WHERE d.project_id=:project AND d.paper_id>:after ORDER BY d.paper_id LIMIT 128
                """),
                    dict(project=str(project), after=after),
                ).all()
                if not page:
                    break
                buffer.seek(0)
                buffer.truncate()
                writer.writerows(page)
                yield buffer.getvalue()
                after = page[-1].paper_id

    return StreamingResponse(
        rows(),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="screening-{project}.csv"'
        },
    )
