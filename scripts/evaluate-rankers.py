# AI-generated evaluation (Codex): freeze query formulation on one review, then score two unseen pools.
import argparse
import csv
import json
import sqlite3
import tempfile
import time
from pathlib import Path

import numpy as np

from papersearch.encoder import Encoder, RECIPE, passage
from papersearch.lexical import LexicalReader
from papersearch.semantic_search import cosine_order
from papersearch.snapshot_format import file_sha

RELEASE = "38b35218e4d0f99621cec5a8a25a0147bb88c654"
SOURCE = f"https://raw.githubusercontent.com/asreview/synergy-dataset/{RELEASE}/datasets/Cohen_2006/output/local/"
POOLS = {
    "Antihistamines": (
        "b94543e803479e978afe12fa21cd7c6e0e661ed12b6684b3ea867c7748432686",
        "antihistamines allergic rhinitis urticaria",
    ),
    "NSAIDS": (
        "198e60d84664838b41c1cc12b9f0693d552643f77016d513c0a2153e74788732",
        "nonsteroidal anti inflammatory drugs NSAIDs arthritis pain",
    ),
    "Triptans": (
        "6c3f4eec28a4451321ca12c1799989efda2e937462052b53865b22598c27a5c6",
        "triptans acute migraine headaches",
    ),
}
FORMULATIONS = {
    "topic": "{}",
    "comparative": "comparative effectiveness safety adverse effects of {}",
}


def rank(candidates, encoder, queries, root):
    # Only ID, title and abstract enter either ranker.
    directory = root / "snapshots" / "pool"
    directory.mkdir(parents=True)
    path = directory / "lexical.sqlite"
    ids = [r[0] for r in candidates]
    lexical_start = time.perf_counter()
    with sqlite3.connect(path) as db:
        db.execute(
            "CREATE VIRTUAL TABLE documents USING fts5(title,abstract,paper_id UNINDEXED,event_id UNINDEXED,text_hash UNINDEXED,tokenize='unicode61')"
        )
        db.executemany(
            "INSERT INTO documents(title,abstract,paper_id) VALUES(?,?,?)",
            [(t, a, i) for i, t, a in candidates],
        )
    db.close()
    lexical_seconds = time.perf_counter() - lexical_start
    manifest = dict(
        snapshot_id="pool",
        files={
            "lexical.sqlite": dict(bytes=path.stat().st_size, sha256=file_sha(path))
        },
    )
    reader = LexicalReader(root)
    reader.path(manifest)
    started = time.perf_counter()
    blocks = []
    for i in range(0, len(ids), 8):
        vectors, _ = encoder.encode(
            [passage(t, a) for _, t, a in candidates[i : i + 8]]
        )
        blocks.append(vectors)
    matrix = np.concatenate(blocks)
    indexing = time.perf_counter() - started
    results = {}
    for name, query in queries.items():
        t = time.perf_counter()
        hits, _ = reader.search(manifest, query, "all", 0, len(ids))
        lexical = [r["paper_id"] for r in hits]
        matched = set(lexical)
        lexical.extend(i for i in ids if i not in matched)
        lexical_ms = 1000 * (time.perf_counter() - t)
        t = time.perf_counter()
        q, _ = encoder.encode([query], query=True)
        order, _ = cosine_order(matrix, q[0], np.arange(len(ids)))
        semantic = [ids[i] for i in order]
        results[name] = dict(
            bm25=(lexical, lexical_ms),
            semantic=(semantic, 1000 * (time.perf_counter() - t)),
        )
    return results, dict(
        semantic_seconds=indexing,
        semantic_matrix_bytes=matrix.nbytes,
        lexical_seconds=lexical_seconds,
        lexical_file_bytes=path.stat().st_size,
    )


def evaluate(name, folder, encoder, forms):
    checksum, topic = POOLS[name]
    path = folder / (name + ".csv")
    if file_sha(path) != checksum:
        raise ValueError("Review file checksum mismatch: " + name)
    with path.open(encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    candidates = sorted(
        (r["record_id"], r["title"].strip(), r["abstract"].strip()) for r in rows
    )
    queries = {f: FORMULATIONS[f].format(topic) for f in forms}
    with tempfile.TemporaryDirectory() as tmp:
        rankings, indexing = rank(candidates, encoder, queries, Path(tmp))
    assert {r["label_included"] for r in rows} == {"0", "1"}
    truth = {r["record_id"] for r in rows if r["label_included"] == "1"}
    metrics = {}
    for form, rankers in rankings.items():
        metrics[form] = {}
        for method, (ids, latency) in rankers.items():
            assert len(ids) == len(set(ids)) == len(rows)
            metrics[form][method] = dict(
                precision_10=len(truth.intersection(ids[:10])) / 10,
                precision_20=len(truth.intersection(ids[:20])) / 20,
                recall_50=len(truth.intersection(ids[:50])) / len(truth),
                recall_100=len(truth.intersection(ids[:100])) / len(truth),
                query_ms=latency,
                first_false_positive=next(
                    (i for i in ids[:10] if i not in truth), None
                ),
                missed_positive_below_100=next(
                    (i for i in ids[100:] if i in truth), None
                ),
                ordered_ids=ids,
            )
    return dict(
        source=SOURCE + name + ".csv",
        sha256=checksum,
        queries=queries,
        size=len(rows),
        included=len(truth),
        title_only=sum(not a for _, _, a in candidates),
        document_encode_seconds=indexing["semantic_seconds"],
        index_build=indexing,
        metrics=metrics,
    )


def main():
    p = argparse.ArgumentParser()
    for arg in ("folder", "model", "output"):
        p.add_argument(arg, type=Path)
    a = p.parse_args()
    encoder = Encoder(a.model)
    dev = evaluate("Antihistamines", a.folder, encoder, FORMULATIONS)
    # Mean development recall@50 across both rankers; simpler topic wins ties.
    frozen = max(
        FORMULATIONS,
        key=lambda f: sum(m["recall_50"] for m in dev["metrics"][f].values()),
    )
    heldout = {
        name: evaluate(name, a.folder, encoder, [frozen])
        for name in ("NSAIDS", "Triptans")
    }
    report = dict(
        release=RELEASE,
        model=RECIPE,
        development=dev,
        frozen=frozen,
        heldout=heldout,
        truth="label_included (final inclusion); rankers receive title and abstract only",
        bm25="unicode61, weights 3:1; nonmatches follow hits by stable record ID",
        cosine="exact normalized CLS dot product, ties by stable record ID",
    )
    a.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
