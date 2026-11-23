# AI-generated code (Codex): drain existing pinned BGE jobs in bounded, restartable batches.
"""Encode the pending papers selected by the reviewer's configured semantic policy."""
import argparse
import os
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from itertools import batched
from pathlib import Path
from time import monotonic

from sqlalchemy import create_engine, text

from papersearch.config import mysql_url
from papersearch.derivation import bind_store, process_batch
from papersearch.encoder import Encoder, RECIPE

PAGE = text("""
    SELECT job_id FROM derived_jobs FORCE INDEX (PRIMARY)
    WHERE job_id > :after AND MOD(job_id, :workers) = :slot
      AND kind=:kind AND model_id=:model_id AND model_revision=:model_revision
      AND preprocessing=:preprocessing
      AND (status='pending' OR (status='leased' AND lease_until<=UTC_TIMESTAMP(6)))
    ORDER BY job_id LIMIT 1024
""")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=int, choices=range(1, 9), default=4)
    parser.add_argument("--root", type=Path, default=Path("/artifacts/derived"))
    args = parser.parse_args()
    engine = create_engine(mysql_url(os.environ), pool_pre_ping=True)
    bind_store(engine, args.root)
    with engine.connect() as conn:
        state = conn.execute(text("SELECT * FROM derivation_state WHERE singleton=1")).mappings().one()
        if any(state[key] != value for key, value in RECIPE.items()):
            raise ValueError("The active recipe differs from the local BGE model")
    start = monotonic()

    def drain(slot):
        encoder = Encoder()
        total = Counter()
        after = 0
        while True:
            with engine.connect() as conn:
                ids = conn.scalars(PAGE, dict(**RECIPE, workers=args.workers, slot=slot, after=after)).all()
            if not ids:
                return total
            for batch in batched(ids, 32):
                result = process_batch(engine, args.root, len(batch), 300, encoder, job_ids=batch)
                total.update(result)
                if result["errors"] or result["lost_leases"]:
                    raise RuntimeError(f"Worker {slot} could not finish its leased jobs: {dict(result)}")
            after = ids[-1]
            print(f"Worker {slot}: {total['completed']} completed in {monotonic()-start:.0f}s", flush=True)

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        total = sum(pool.map(drain, range(args.workers)), Counter())
    with engine.connect() as conn:
        incomplete = conn.scalar(text("""
            SELECT COUNT(*) FROM derived_jobs WHERE kind=:kind AND model_id=:model_id
            AND model_revision=:model_revision AND preprocessing=:preprocessing AND status!='done'
        """), RECIPE)
    if incomplete:
        raise RuntimeError(f"{incomplete} jobs still incomplete; inspect the queue before publishing")
    print(f"Finished in {monotonic()-start:.1f}s: {dict(total)}", flush=True)


if __name__ == "__main__":
    main()
