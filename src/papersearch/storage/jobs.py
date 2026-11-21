# AI-generated code (Codex): database-clock leases and fenced completion for derived jobs.
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.engine import Engine

MAX_ATTEMPTS = 3


def claim_job(engine: Engine, lease_seconds: int = 60) -> dict | None:
    if not 1 <= lease_seconds <= 86400:
        raise ValueError("lease_seconds must be between 1 and 86400")
    with engine.connect().execution_options(isolation_level="READ COMMITTED") as conn:
        while True:
            with conn.begin():
                job = (
                    conn.execute(
                        text("""
                    SELECT job_id,paper_id,text_hash,kind,model_id,model_revision,preprocessing,attempts
                    FROM derived_jobs WHERE status='pending'
                        OR (status='leased' AND lease_until<=UTC_TIMESTAMP(6))
                    ORDER BY job_id LIMIT 1 FOR UPDATE SKIP LOCKED
                """)
                    )
                    .mappings()
                    .first()
                )
                if job is None:
                    return None
                if job["attempts"] >= MAX_ATTEMPTS:
                    conn.execute(
                        text("""
                        UPDATE derived_jobs SET status='failed',lease_until=NULL,lease_token=NULL,
                            last_error='Final lease expired' WHERE job_id=:id
                    """),
                        {"id": job["job_id"]},
                    )
                    continue
                token = str(uuid4())
                conn.execute(
                    text("""
                    UPDATE derived_jobs SET status='leased',attempts=attempts+1,lease_token=:token,
                        lease_until=TIMESTAMPADD(SECOND,:ttl,UTC_TIMESTAMP(6)) WHERE job_id=:id
                """),
                    dict(token=token, ttl=lease_seconds, id=job["job_id"]),
                )
                return {**job, "attempts": job["attempts"] + 1, "lease_token": token}


def finish_job(
    engine: Engine,
    job_id: int,
    token: str,
    *,
    error: str | None = None,
    retryable: bool = True,
) -> bool:
    """Only the holder of a live lease may finish or relinquish this attempt."""
    with engine.begin() as conn:
        args = dict(id=job_id, token=token, error=error)
        attempts = conn.scalar(
            text("""
            SELECT attempts FROM derived_jobs WHERE job_id=:id FOR UPDATE
        """),
            args,
        )
        if attempts is None:
            return False
        status = "done"
        if error is not None:
            status = "pending" if retryable and attempts < MAX_ATTEMPTS else "failed"
        result = conn.execute(
            text("""
            UPDATE derived_jobs SET status=:status,last_error=:error,lease_token=NULL,lease_until=NULL,
                completed_at=IF(:status='done',UTC_TIMESTAMP(6),NULL)
            WHERE job_id=:id AND status='leased' AND lease_token=:token AND lease_until>UTC_TIMESTAMP(6)
        """),
            {**args, "status": status},
        )
        return result.rowcount == 1
