# AI-generated benchmark (Codex): two disjoint seeded samples from the verified real FTS corpus.
import argparse
import hashlib
import json
import platform
import random
import sqlite3
import statistics
import subprocess
import time
from pathlib import Path

import psutil

from papersearch.encoder import Encoder, MAX_TOKENS, RECIPE, passage


def run(directory, model, output):
    manifest = json.loads((directory / "manifest.json").read_bytes())
    assert manifest["coverage"]["metadata"] == 100000
    start = time.perf_counter()
    encoder = Encoder(model)
    loaded = time.perf_counter() - start
    db = sqlite3.connect(
        (directory / "lexical.sqlite").resolve().as_uri() + "?mode=ro&immutable=1",
        uri=True,
    )
    seed = 54406
    sample = random.Random(seed).sample(range(1, 100001), 24 + 5000 + 8000)
    report = dict(
        source_commit=subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip(),
        model=RECIPE,
        corpus_sha256=manifest["corpus_sha256"],
        snapshot_id=manifest["snapshot_id"],
        seed=seed,
        batch=8,
        threads=2,
        max_tokens=MAX_TOKENS,
        model_load_seconds=loaded,
        host=platform.platform(),
        python=platform.python_version(),
        runs={},
    )
    for label, ids in (
        ("warmup", sample[:24]),
        ("representative", sample[24:5024]),
        ("sustained", sample[5024:]),
    ):
        began = time.perf_counter()
        lengths, times, empty, text_digest = [], [], 0, hashlib.sha256()
        for i in range(0, len(ids), 8):
            texts = []
            for number in ids[i : i + 8]:
                title, abstract, text_hash = db.execute(
                    "SELECT title,abstract,text_hash FROM documents WHERE rowid=?",
                    (number,),
                ).fetchone()
                texts.append(passage(title, abstract))
                text_digest.update(bytes.fromhex(text_hash))
                empty += not bool(abstract)
            t = time.perf_counter()
            _, counts = encoder.encode(texts)
            times.append(time.perf_counter() - t)
            lengths.extend(counts)
            if (i + 8) % 1000 == 0:
                print(label, i + 8, flush=True)
        elapsed = time.perf_counter() - began
        report["runs"][label] = dict(
            count=len(ids),
            ordered_text_digest=text_digest.hexdigest(),
            mean_tokens=statistics.mean(lengths),
            median_tokens=statistics.median(lengths),
            max_tokens=max(lengths),
            truncated=sum(n > MAX_TOKENS for n in lengths),
            title_only=empty,
            elapsed_seconds=elapsed,
            encode_seconds=sum(times),
            texts_per_second=len(ids) / sum(times),
            batch_seconds=times,
            process_peak_rss_bytes=psutil.Process().memory_info().peak_wset,
        )
        output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    db.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    for arg in ("directory", "model", "output"):
        p.add_argument(arg, type=Path)
    a = p.parse_args()
    run(a.directory, a.model, a.output)
