# AI-generated code (Codex): bounded leased fixture workers and verified, content-addressed shards.
import hashlib
import os
import re
import json
import struct
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

from sqlalchemy import text

from .archive import sync_dir, write_atomic
from .contracts import digest
from .encoder import (
    KIND,
    PAYLOAD_BYTES,
    RECIPE,
    eligibility_sql,
    passage,
    unpack_vector,
)
from .storage.jobs import claim_job, finish_locked

TEST_KIND = "test-digest-v1"
FIELDS = ("kind", "model_id", "model_revision", "preprocessing")
WIDTH = 32


def bind_store(engine, root):
    """Fence writers pointed at a different physical artifact directory."""
    with engine.begin() as conn:
        identity = conn.scalar(
            text("SELECT store_id FROM derivation_state WHERE singleton=1 FOR UPDATE")
        )
        if identity:
            if not (root / ".stores" / identity).is_dir():
                raise ValueError(
                    "The selected directory is not the shared artifact store for this database"
                )
            return
        identity = uuid4().hex
        marker = root / ".stores" / identity
        marker.mkdir(parents=True)
        sync_dir(marker.parent)
        sync_dir(root)
        conn.execute(
            text("UPDATE derivation_state SET store_id=:id WHERE singleton=1"),
            dict(id=identity),
        )


def cache_key(text_hash, recipe):
    return digest([text_hash, *(recipe[field] for field in FIELDS)])


def fixture_vector(key):
    return hashlib.sha256(
        b"PaperSearch deterministic test bytes\0" + bytes.fromhex(key)
    ).digest()


def configure(
    engine, model_id="fixture-digest", model_revision="v1", preprocessing="text-v1"
):
    recipe = dict(
        kind=TEST_KIND,
        model_id=model_id,
        model_revision=model_revision,
        preprocessing=preprocessing,
    )
    for value, limit in ((model_id, 160), (model_revision, 64), (preprocessing, 32)):
        if not re.fullmatch(r"[A-Za-z0-9._/-]{1,%d}" % limit, value):
            raise ValueError("Invalid fixture recipe identity")
    return configure_recipe(engine, recipe)


def configure_recipe(engine, recipe):
    with engine.begin() as conn:
        conn.execute(
            text("""UPDATE derivation_state SET recipe_epoch=recipe_epoch+1, kind=:kind,
            model_id=:model_id,model_revision=:model_revision,preprocessing=:preprocessing
            WHERE singleton=1 AND (kind,model_id,model_revision,preprocessing)<>
                (:kind,:model_id,:model_revision,:preprocessing)"""),
            recipe,
        )
    # Install the recipe first. Imports arriving during backfill enqueue their own winner.
    after = ""
    while True:
        with engine.begin() as conn:
            live = (
                conn.execute(
                    text(
                        "SELECT kind,model_id,model_revision,preprocessing FROM derivation_state "
                        "WHERE singleton=1 FOR SHARE"
                    )
                )
                .mappings()
                .one()
            )
            if dict(live) != recipe:
                raise ValueError(
                    "Another process selected a different recipe during backfill"
                )
            page = conn.execute(
                text(f"""SELECT c.paper_id,r.text_hash FROM current_revisions c
                JOIN paper_revisions r ON r.event_id=c.event_id WHERE {eligibility_sql("c.paper_id", ":kind")}
                AND c.paper_id>:after ORDER BY c.paper_id LIMIT 128"""),
                dict(after=after, **recipe),
            ).all()
            if not page:
                return recipe
            conn.execute(
                text("""INSERT INTO derived_jobs(paper_id,text_hash,kind,model_id,model_revision,preprocessing)
                VALUES(:paper,:hash,:kind,:model_id,:model_revision,:preprocessing)
                ON DUPLICATE KEY UPDATE job_id=derived_jobs.job_id"""),
                [dict(paper=p, hash=h, **recipe) for p, h in page],
            )
            after = page[-1].paper_id


def crash_at(point):
    if (
        os.environ.get("PAPERSEARCH_TEST_FAULTS") == "1"
        and os.environ.get("PAPERSEARCH_TEST_CRASH") == point
    ):
        print(f"TEST process exit at {point}", flush=True)
        os._exit(86)


def shard_path(root, checksum):
    if not re.fullmatch(r"[0-9a-f]{64}", checksum):
        raise ValueError("Invalid shard checksum")
    return root / "shards" / checksum[:2] / checksum


def verified_vector(root, artifact, buffers, *, semantic=False):
    width = PAYLOAD_BYTES if semantic else WIDTH
    checksum = artifact["shard_sha"]
    if checksum not in buffers:
        with shard_path(root, checksum).open("rb") as source:
            data = source.read(64 * width + 1)
        if (
            len(data) > 64 * width
            or len(data) % width
            or hashlib.sha256(data).hexdigest() != checksum
        ):
            raise ValueError(f"Invalid artifact shard {checksum}")
        buffers[checksum] = data
    start = width * artifact["slot"]
    vector = buffers[checksum][start : start + width]
    if (
        len(vector) != width
        or hashlib.sha256(vector).hexdigest() != artifact["vector_sha"]
        or (not semantic and vector != fixture_vector(artifact["cache_key"]))
    ):
        raise ValueError("Cached test vector does not match its identity")
    if semantic:
        unpack_vector(vector)
    return vector


def store_shard(root, generated, *, semantic=False):
    data = b"".join(generated.values())
    sha = hashlib.sha256(data).hexdigest()
    path = shard_path(root, sha)
    path.parent.mkdir(parents=True, exist_ok=True)
    reused = path.exists()
    if reused:
        if path.stat().st_size != len(data) or path.read_bytes() != data:
            raise ValueError(f"Existing shard failed verification: {sha}")
    else:
        write_atomic(path, data)
        sync_dir(path.parent.parent)
    locations, buffers = {}, {}
    for slot, (key, vector) in enumerate(generated.items()):
        location = dict(
            cache_key=key,
            shard_sha=sha,
            slot=slot,
            vector_sha=hashlib.sha256(vector).hexdigest(),
        )
        verified_vector(root, location, buffers, semantic=semantic)
        locations[key] = location
    crash_at("artifact-renamed")
    return locations, reused


def process_batch(engine, root, limit, lease_seconds, encoder=None):
    jobs = []
    for _ in range(limit):
        job = claim_job(engine, lease_seconds, kind=KIND if encoder else TEST_KIND)
        if job is None:
            break
        job["cache_key"] = cache_key(job["text_hash"], job)
        jobs.append(job)
    counts = Counter(claimed=len(jobs))
    if not jobs:
        return counts
    if encoder is not None:
        crash_at("semantic-batch-claimed")
    generated, locations, errors, buffers = {}, {}, {}, {}
    with engine.connect() as conn:
        missing = {}
        for key, job in {job["cache_key"]: job for job in jobs}.items():
            artifact = (
                conn.execute(
                    text("SELECT * FROM derived_artifacts WHERE cache_key=:key"),
                    dict(key=key),
                )
                .mappings()
                .first()
            )
            if artifact is None:
                if encoder is None:
                    generated[key] = fixture_vector(key)
                else:
                    if any(job[field] != RECIPE[field] for field in FIELDS):
                        raise ValueError("Unsupported semantic recipe")
                    raw = conn.scalar(
                        text("""SELECT record_json FROM paper_revisions
                        WHERE paper_id=:paper_id AND text_hash=:text_hash LIMIT 1"""),
                        job,
                    )
                    payload = json.loads(raw)["payload"]
                    if (
                        digest(["text-v1", payload["title"], payload["abstract"]])
                        != job["text_hash"]
                    ):
                        raise ValueError("Leased job text hash mismatch")
                    missing[key] = passage(payload["title"], payload["abstract"])
                continue
            try:
                verified_vector(root, artifact, buffers, semantic=encoder is not None)
            except (OSError, ValueError) as exc:
                errors[key] = str(exc)[:2048]
    if missing:
        vectors, lengths = encoder.encode(list(missing.values()))
        for key, vector, length in zip(missing, vectors, lengths, strict=True):
            generated[key] = struct.pack("<I", length) + vector.tobytes()
    if generated:
        try:
            locations, reused = store_shard(
                root, generated, semantic=encoder is not None
            )
            counts["shards_reused"] += int(reused)
        except (OSError, ValueError) as exc:
            errors.update(dict.fromkeys(generated, str(exc)[:2048]))
    counts["computed"] = len(generated)
    counts["hits"] = sum(job["cache_key"] not in errors for job in jobs) - len(
        locations
    )
    for job in jobs:
        key = job["cache_key"]
        with engine.begin() as conn:
            # Lock in the same order as import and backfill (settings, then job).
            if key in locations:
                conn.execute(
                    text(
                        "SELECT singleton FROM derivation_state WHERE singleton=1 FOR UPDATE"
                    )
                ).one()
            if not finish_locked(
                conn, job["job_id"], job["lease_token"], error=errors.get(key)
            ):
                counts["lost_leases"] += 1
                continue
            if key in errors:
                counts["errors"] += 1
                continue
            if key in locations and not conn.scalar(
                text(
                    "SELECT cache_key FROM derived_artifacts WHERE cache_key=:key FOR SHARE"
                ),
                dict(key=key),
            ):
                conn.execute(
                    text("""INSERT INTO derived_artifacts
                    (cache_key,text_hash,kind,model_id,model_revision,preprocessing,shard_sha,slot,vector_sha)
                    VALUES (:cache_key,:text_hash,:kind,:model_id,:model_revision,:preprocessing,:shard_sha,:slot,:vector_sha)"""),
                    {**job, **locations[key]},
                )
                conn.execute(
                    text(
                        "UPDATE derivation_state SET artifact_epoch=artifact_epoch+1 WHERE singleton=1"
                    )
                )
            counts["completed"] += 1
    return counts


def run_workers(
    engine, root: Path, workers=2, batch_size=16, max_jobs=1024, lease_seconds=60
):
    if not (
        1 <= workers <= 4
        and 1 <= batch_size <= 64
        and 1 <= max_jobs <= 100000
        and 1 <= lease_seconds <= 86400
    ):
        raise ValueError(
            "Expected 1–4 workers, 1–64 jobs per shard, 1–100000 job attempts and a 1–86400 second lease"
        )
    bind_store(engine, root)
    total = Counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        while (remaining := max_jobs - total["claimed"]) > 0:
            futures = [
                pool.submit(
                    process_batch,
                    engine,
                    root,
                    min(batch_size, remaining - i),
                    lease_seconds,
                )
                for i in range(0, min(workers * batch_size, remaining), batch_size)
            ]
            claimed = 0
            for future in futures:
                counts = future.result()
                claimed += counts["claimed"]
                total.update(counts)
            if not claimed or total["errors"]:
                break
    return dict(total)


def retry_job(engine, job_id, kind=TEST_KIND):
    with engine.begin() as conn:
        result = conn.execute(
            text("""UPDATE derived_jobs SET status='pending',attempts=0 WHERE job_id=:job
            AND kind=:kind AND status='failed'"""),
            dict(job=job_id, kind=kind),
        )
        if result.rowcount != 1:
            raise ValueError("Expected a failed job of the selected kind")


def run_semantic(engine, root, batch_size, max_jobs, lease_seconds):
    from .encoder import Encoder

    if (
        not 1 <= batch_size <= 64
        or not 1 <= max_jobs <= 100000
        or not 1 <= lease_seconds <= 86400
    ):
        raise ValueError("Invalid semantic worker batch, attempt or lease limit")
    bind_store(engine, root)
    encoder = Encoder()  # Validate dependencies before taking a lease.
    total = Counter()
    while total["claimed"] < max_jobs:
        counts = process_batch(
            engine,
            root,
            min(batch_size, max_jobs - total["claimed"]),
            lease_seconds,
            encoder,
        )
        total.update(counts)
        if not counts["claimed"] or counts["errors"] or counts["lost_leases"]:
            break
        crash_at("semantic-batch-committed")
    return dict(total)
