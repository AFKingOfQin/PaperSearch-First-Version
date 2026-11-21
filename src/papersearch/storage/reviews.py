# AI-generated code (Codex): project-specific decisions tied to the revision actually reviewed.
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.engine import Engine


def create_project(engine: Engine, topic: str, criteria: str) -> str:
    if not topic.strip():
        raise ValueError("A review project needs a topic")
    project = str(uuid4())
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO review_projects (project_id,topic,criteria) VALUES (:id,:topic,:criteria)"
            ),
            dict(id=project, topic=topic, criteria=criteria),
        )
    return project


def save_decision(
    engine: Engine,
    project: str,
    paper: str,
    reviewed_event: str,
    label: str,
    note: str,
    request_id: str,
) -> int:
    if label not in ("relevant", "irrelevant", "uncertain"):
        raise ValueError("Unknown screening label")
    args = dict(
        project=project,
        paper=paper,
        reviewed=reviewed_event,
        label=label,
        note=note,
        request=str(UUID(request_id)),
    )
    with engine.begin() as conn:
        if not conn.scalar(
            text("SELECT paper_id FROM papers WHERE paper_id=:paper FOR UPDATE"), args
        ):
            raise ValueError("Unknown paper")
        prior = conn.execute(
            text("""
            SELECT history_id,reviewed_event_id,label,note FROM screening_history
            WHERE project_id=:project AND paper_id=:paper AND request_id=:request
        """),
            args,
        ).first()
        if prior:
            if tuple(prior[1:]) != (reviewed_event, label, note):
                raise ValueError(
                    "Decision request ID was reused with different content"
                )
            return prior.history_id
        if not conn.scalar(
            text("SELECT project_id FROM review_projects WHERE project_id=:project"),
            args,
        ):
            raise ValueError("Unknown review project")
        if not conn.scalar(
            text("""
            SELECT event_id FROM paper_revisions WHERE event_id=:reviewed AND paper_id=:paper
        """),
            args,
        ):
            raise ValueError("Reviewed revision does not belong to this paper")
        history = conn.execute(
            text("""
            INSERT INTO screening_history
                (project_id,paper_id,reviewed_event_id,label,note,request_id,saved_at)
            VALUES (:project,:paper,:reviewed,:label,:note,:request,UTC_TIMESTAMP(6))
        """),
            args,
        ).lastrowid
        conn.execute(
            text("""
            INSERT INTO screening_decisions VALUES (:project,:paper,:history)
            ON DUPLICATE KEY UPDATE history_id=:history
        """),
            {**args, "history": history},
        )
        return history


def read_decision(engine: Engine, project: str, paper: str) -> dict | None:
    with engine.connect() as conn:
        return decision_on_connection(conn, project, paper)


def decision_on_connection(conn, project: str, paper: str) -> dict | None:
    row = (
        conn.execute(
            text("""
        SELECT h.history_id,h.label,h.note,h.reviewed_event_id,h.saved_at,
               r.text_hash AS reviewed_text_hash,v.text_hash AS current_text_hash
        FROM screening_decisions d JOIN screening_history h ON d.history_id=h.history_id
        JOIN paper_revisions r ON r.event_id=h.reviewed_event_id
        LEFT JOIN current_revisions c ON c.paper_id=d.paper_id
        LEFT JOIN paper_revisions v ON v.event_id=c.event_id
        WHERE d.project_id=:project AND d.paper_id=:paper
    """),
            dict(project=project, paper=paper),
        )
        .mappings()
        .first()
    )
    if row is None:
        return None
    return {
        **row,
        "needs_reconsideration": row["reviewed_text_hash"] != row["current_text_hash"],
    }
