# AI-generated verification (Codex): live semantic relevance checks and an independent audit of published mappings.
import argparse
import hashlib
import json
import struct
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import requests

from papersearch.encoder import Encoder, passage

TARGETS = {"arxiv:2406.18139", "arxiv:2410.23317"}
CACHE_QUERIES = (
    "VLM KV Cache",
    "Compressing the attention key and value cache to reduce memory during inference in vision-language models",
)
CONVERSATIONAL = "How can an image-and-text transformer use less memory for the attention keys and values it saves during generation?"
INVENTED_OBJECTS = "detecting invented objects in image descriptions"
TOPICS = {
    "Why do image captioning models hallucinate objects that are absent from a picture?": (
        "hallucinat",
    ),
    "answering questions about charts and diagrams": ("chart", "diagram"),
    "prediction intervals with conformal inference": ("conformal",),
    "discovering causal relationships from observational data": ("causal",),
    "Bayesian optimization for expensive black-box functions": (
        "bayesian optim",
        "black-box",
        "black box",
    ),
}


def query_reviewer(base):
    def get(path, **params):
        response = requests.get(base + path, params=params, timeout=100)
        response.raise_for_status()
        return response.json()

    manifest = get("/status")["active"]
    scope = manifest["semantic_scope"]
    assert scope["cohorts"] and all(
        c["eligible"] and c["eligible"] == c["indexed"] for c in scope["cohorts"]
    )
    project = get("/projects")[0]["project_id"]
    results = {}

    def search(query, offset=0):
        page = get(
            f"/projects/{project}/search",
            q=query,
            ranker="semantic",
            limit=100,
            offset=offset,
            snapshot=manifest["snapshot_id"],
        )
        assert page["ranker"] == "semantic" and page["fallback_reason"] is None
        assert page["coverage"] == manifest["coverage"]
        return page["results"]

    for query in (*CACHE_QUERIES, *TOPICS, CONVERSATIONAL, INVENTED_OBJECTS):
        rows = search(query)
        assert rows and all(a["score"] >= b["score"] for a, b in zip(rows, rows[1:]))
        results[query] = dict(
            top_five=[
                {k: h[k] for k in ("paper_id", "rank", "title", "score")}
                for h in rows[:5]
            ]
        )
        # Record harder wordings as well, including named papers far below the first page.
        if query == CONVERSATIONAL:
            for offset in range(100, min(1000, manifest["coverage"]["semantic"]), 100):
                if TARGETS <= {r["paper_id"] for r in rows}:
                    break
                rows.extend(search(query, offset))
            results[query]["target_ranks_in_first_1000"] = {
                r["paper_id"]: r["rank"] for r in rows if r["paper_id"] in TARGETS
            }
        if query == INVENTED_OBJECTS:
            results[query]["first_title_mentioning_hallucination_in_top_100"] = next(
                (r["rank"] for r in rows if "hallucinat" in r["title"].lower()), None
            )
        if query in CACHE_QUERIES:
            ranks = {h["paper_id"]: h["rank"] for h in rows if h["paper_id"] in TARGETS}
            results[query]["target_ranks"] = ranks
            assert ranks.keys() == TARGETS and min(ranks.values()) <= 10, (
                query,
                ranks,
                rows[:5],
            )
            if query == "VLM KV Cache":
                assert max(ranks.values()) <= 10, ranks
        elif query in TOPICS:
            assert any(
                token in h["title"].lower() for token in TOPICS[query] for h in rows[:5]
            ), (query, rows[:5])
    return dict(
        snapshot=manifest["snapshot_id"],
        corpus_sha256=manifest["corpus_sha256"],
        recipe=manifest["recipe"],
        coverage=manifest["coverage"],
        scope=scope,
        queries=results,
    )


def audit(folder, previous, report, model):
    path = folder / report["snapshot"]
    columns = ["paper_id", "row_id", "event_id", "text_hash", "cache_key", "vector_sha"]

    def vectors(directory):
        return {
            row["paper_id"]: row
            for row in pq.read_table(
                directory / "semantic.parquet", columns=columns
            ).to_pylist()
        }

    old, new = vectors(folder / previous), vectors(path)
    assert len(new) == report["coverage"]["semantic"]
    assert all(new.get(paper) == row for paper, row in old.items())
    eligible = set()
    checked = 0
    target_rows = []
    data = [
        "paper_id",
        "row_id",
        "event_id",
        "embedding_text_hash",
        "categories",
        "published",
    ]
    with pq.ParquetFile(path / "corpus.parquet") as corpus:
        for batch in corpus.iter_batches(columns=data):
            for row in batch.to_pylist():
                p, cats, day = (
                    row["paper_id"],
                    row["categories"],
                    row["published"] or "",
                )
                if (
                    day >= "2024-01-01"
                    and {"cs.CL", "cs.CV"}.issubset(cats)
                    or "2024-01-01" <= day < "2025-01-01"
                    and "stat.ML" in cats
                ):
                    eligible.add(p)
                if p in new:
                    checked += 1
                    assert (
                        row["row_id"],
                        row["event_id"],
                        row["embedding_text_hash"],
                    ) == (new[p]["row_id"], new[p]["event_id"], new[p]["text_hash"])
                if p in TARGETS:
                    target_rows.append(row)
    assert checked == len(new)
    assert eligible <= new.keys() and TARGETS <= eligible
    assert len(target_rows) == len(TARGETS)
    encoder = Encoder(model)
    recorded = pq.read_table(
        path / "semantic.parquet", filters=[("paper_id", "in", sorted(TARGETS))]
    ).to_pylist()
    texts = pq.read_table(
        path / "corpus.parquet",
        columns=["paper_id", "title", "abstract"],
        filters=[("paper_id", "in", sorted(TARGETS))],
    ).to_pylist()
    similarities = {}
    for stored in recorded:
        paper = stored["paper_id"]
        source = next(s for s in texts if s["paper_id"] == paper)
        vector, lengths = encoder.encode([passage(source["title"], source["abstract"])])
        raw = stored["value"]
        assert stored["tokens"] == lengths[0]
        assert (
            hashlib.sha256(struct.pack("<I", lengths[0]) + raw).hexdigest()
            == new[paper]["vector_sha"]
        )
        similarities[paper] = float(np.dot(vector[0], np.frombuffer(raw, dtype="<f4")))
        assert similarities[paper] > 0.99999
    return dict(
        previous=previous,
        unchanged_prior_mappings=len(old),
        checked_mappings=len(new),
        distinct_vector_keys=len({r["cache_key"] for r in new.values()}),
        cohort_union=len(eligible),
        missing_cohort_papers=len(eligible - new.keys()),
        target_rows=target_rows,
        reencoded_cosine=similarities,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("base")
    parser.add_argument("output", type=Path)
    parser.add_argument("--snapshots", type=Path)
    parser.add_argument("--previous")
    parser.add_argument("--model", type=Path)
    args = parser.parse_args()
    result = query_reviewer(args.base.rstrip("/"))
    if args.snapshots:
        if not args.previous or not args.model:
            parser.error("--snapshots requires --previous and --model")
        result["audit"] = audit(args.snapshots, args.previous, result, args.model)
    args.output.write_text(
        json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf8"
    )
    print("PASS semantic reviewer", result["snapshot"], result["coverage"])
