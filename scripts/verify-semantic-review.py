# AI-generated verification (Codex): exercise both rankers and explicit model fallback through deployed HTTP.
import argparse
import json
import os
import sqlite3
import time
from contextlib import closing
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import requests
from streamlit.testing.v1 import AppTest

from papersearch.encoder import Encoder, passage
from review_demo import wait_http


def verify_scores(directory, model, hits):
    rows = pq.read_table(
        directory / "semantic.parquet",
        columns=["row_id", "paper_id", "event_id", "value"],
    ).to_pylist()
    matrix = np.frombuffer(b"".join(r["value"] for r in rows), dtype="<f4").reshape(
        len(rows), -1
    )
    encoder = Encoder(model)
    query, _ = encoder.encode(["quantum information"], query=True)
    scores = matrix @ query[0]
    # Published rows already ascend by corpus row ID, the production tie-breaker.
    order = np.argsort(-scores, kind="stable")[: len(hits)]
    for hit, i in zip(hits, order, strict=True):
        assert (hit["paper_id"], hit["event_id"]) == (
            rows[i]["paper_id"],
            rows[i]["event_id"],
        )
        assert abs(hit["score"] - float(scores[i])) < 1e-5
    first = rows[order[0]]
    uri = (directory / "lexical.sqlite").resolve().as_uri() + "?mode=ro&immutable=1"
    with closing(sqlite3.connect(uri, uri=True)) as db:
        title, abstract = db.execute(
            "SELECT title,abstract FROM documents WHERE rowid=?", (first["row_id"],)
        ).fetchone()
    encoded, _ = encoder.encode([passage(title, abstract)])
    similarity = float(encoded[0] @ matrix[order[0]])
    assert similarity > 0.99999
    return dict(
        independently_scored=len(rows),
        matched_top_ids=[r["paper_id"] for r in hits],
        reencoded_passage_cosine=similarity,
    )


def check(base, no_model, output):
    for server in (base, no_model):
        wait_http(server + "/health")
    topic = "Semantic screening verification"
    project = requests.post(base + "/projects", json=dict(topic=topic), timeout=20)
    project.raise_for_status()
    project = project.json()["project_id"]
    route = f"/projects/{project}/search"

    def search(server, **params):
        t = time.perf_counter()
        r = requests.get(
            server + route, params={"q": "quantum information", **params}, timeout=120
        )
        r.raise_for_status()
        return r.json(), 1000 * (time.perf_counter() - t)

    bm, lex_time = search(base, ranker="bm25")
    semantic, cold = search(base, ranker="semantic", limit=2)
    assert semantic["ranker"] == "semantic" and semantic["fallback_reason"] is None
    counts = semantic["coverage"]
    assert (
        0
        < counts["semantic"]
        == counts["semantic_eligible"]
        < counts["metadata"]
        == counts["lexical"]
        == 100000
    )
    warm = []
    for _ in range(5):
        again, ms = search(
            base, ranker="semantic", limit=2, snapshot=semantic["snapshot_id"]
        )
        assert again["results"] == semantic["results"]
        warm.append(ms)
    next_page, _ = search(
        base, ranker="semantic", offset=2, limit=2, snapshot=semantic["snapshot_id"]
    )
    first = semantic["results"][0]
    assert next_page["results"][0]["rank"] == 3
    assert not {r["paper_id"] for r in semantic["results"]}.intersection(
        r["paper_id"] for r in next_page["results"]
    )
    detail = requests.get(
        base + "/papers/detail",
        params=dict(paper=first["paper_id"], event=first["event_id"]),
        timeout=10,
    ).json()
    assert detail["record"]["event_id"] == first["event_id"]
    assert search(base, ranker="semantic", q="a " * 160, limit=1)[0]["query_truncated"]
    fallback, _ = search(no_model, ranker="semantic")
    assert (
        fallback["ranker"] == "bm25"
        and fallback["fallback_reason"]
        and fallback["results"] == bm["results"]
    )
    os.environ["PAPERSEARCH_API"] = base
    app = AppTest.from_file(
        str(Path(__file__).resolve().parents[1] / "src/papersearch/reviewer.py"),
        default_timeout=90,
    ).run()
    next(w for w in app.selectbox if w.label == "Ranking").set_value("semantic")
    app.main.text_input[0].set_value("quantum information").run()
    assert not app.exception and app.radio[0].value == first["event_id"]
    assert any("Cosine ranking" in w.value for w in app.caption)
    assert any(
        f"{counts['semantic']:,} of {counts['semantic_eligible']:,} eligible" in w.value
        and f"{counts['semantic_truncated']:,} indexed texts shortened" in w.value
        for w in app.caption
    )
    artifact_dir = Path(os.environ["ARTIFACT_DIR"])
    audit = verify_scores(
        artifact_dir / "derived" / "snapshots" / semantic["snapshot_id"],
        artifact_dir / "models/bge-small-en-v1.5",
        semantic["results"] + next_page["results"],
    )
    result = dict(
        snapshot=semantic["snapshot_id"],
        coverage=counts,
        top_hit=first,
        bm25_top=bm["results"][0],
        cold_semantic_http_ms=cold,
        warm_semantic_http_ms=warm,
        lexical_http_ms=lex_time,
        fallback=fallback["fallback_reason"],
        ui_top_event=app.radio[0].value,
        independent_check=audit,
    )
    output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(
        "PASS exact semantic ranking, stable pagination, indexed revision, truncation, Streamlit selection and model-unavailable BM25 fallback"
    )


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("base")
    p.add_argument("no_model")
    p.add_argument("output", type=Path)
    a = p.parse_args()
    check(a.base, a.no_model, a.output)
