# AI-generated code (Codex): schedule calendar/category slices through the same fenced vector jobs as ingestion.
from collections import Counter
from itertools import batched
from time import perf_counter

import pyarrow.parquet as pq
from sqlalchemy import bindparam, text

from .derivation import FIELDS, bind_store, enqueue_jobs, process_batch
from .encoder import RECIPE, Encoder, eligible, eligibility_sql
from .snapshot_format import file_sha
from .snapshots import published_manifest


def selected_revisions(path, start, before, categories):
    columns = ["paper_id", "event_id", "published", "categories"]
    with pq.ParquetFile(path) as parquet:
        for batch in parquet.iter_batches(batch_size=4096, columns=columns):
            for row in batch.to_pylist():
                if (
                    start <= (row["published"] or "") < before
                    and categories.issubset(row["categories"])
                    and eligible(row["paper_id"], row)
                ):
                    yield row["paper_id"], row["event_id"]


def schedule_current(engine, revisions):
    # Validate the published revision against the current winner before queuing.
    # The shared settings lock also fences imports and a concurrent recipe change.
    with engine.begin() as conn:
        state = (
            conn.execute(
                text("SELECT * FROM derivation_state WHERE singleton=1 FOR SHARE")
            )
            .mappings()
            .one()
        )
        if any(state[k] != RECIPE[k] for k in FIELDS):
            raise ValueError("Configure the pinned semantic recipe before backfilling")
        rows = conn.execute(
            text(f"""SELECT c.paper_id,c.event_id,r.text_hash FROM current_revisions c
            JOIN paper_revisions r ON r.event_id=c.event_id WHERE c.paper_id IN :ids
            AND {eligibility_sql("c.paper_id", ":kind")} ORDER BY c.paper_id""").bindparams(
                bindparam("ids", expanding=True)
            ),
            dict(ids=list(revisions), **RECIPE),
        ).all()
        papers = [(p, h) for p, e, h in rows if revisions[p] == e]
        if not papers:
            return []
        enqueue_jobs(conn, RECIPE, papers)
        return conn.scalars(
            text("""SELECT job_id FROM derived_jobs WHERE (paper_id,text_hash) IN :keys
            AND kind=:kind AND model_id=:model_id AND model_revision=:model_revision
            AND preprocessing=:preprocessing ORDER BY job_id""").bindparams(
                bindparam("keys", expanding=True)
            ),
            dict(keys=papers, **RECIPE),
        ).all()


def backfill(engine, root, start, before, categories, batch_size):
    if before <= start or not 1 <= batch_size <= 64:
        raise ValueError(
            "Expected an increasing date range and batch size between 1 and 64"
        )
    started = perf_counter()
    bind_store(engine, root)
    with engine.connect() as conn:
        snapshot = conn.scalar(
            text("SELECT active_snapshot FROM derivation_state WHERE singleton=1")
        )
        manifest = published_manifest(conn, root, snapshot)
    if not manifest:
        raise ValueError("Publish the corpus before scheduling a collection slice")
    path = root / "snapshots" / snapshot / "corpus.parquet"
    if file_sha(path) != manifest["files"]["corpus.parquet"]["sha256"]:
        raise ValueError("Published corpus checksum mismatch")
    encoder = Encoder()
    total = Counter()
    rows = selected_revisions(
        path, start.isoformat(), before.isoformat(), set(categories)
    )
    for batch in batched(rows, batch_size):
        ids = schedule_current(engine, dict(batch))
        total["selected"] += len(batch)
        total["superseded"] += len(batch) - len(ids)
        counts = process_batch(engine, root, len(ids), 180, encoder, job_ids=ids)
        total.update(counts)
        if counts["errors"] or counts["lost_leases"]:
            break
    return dict(
        snapshot=snapshot,
        start=str(start),
        before=str(before),
        categories=sorted(set(categories)),
        seconds=round(perf_counter() - started, 3),
        **total,
    )
