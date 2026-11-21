# AI-generated code (Codex): exercise the reviewer against live HTTP, Streamlit and isolated persistent services.
import argparse
import csv
import io
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

import requests
from streamlit.testing.v1 import AppTest

REPO = Path(__file__).resolve().parents[1]
UI = REPO / "src/papersearch/reviewer.py"


def workflow(base, output, restart, revise=None, rebuild=None, query="river"):
    def http(method, path, expected=200, **kwargs):
        res = requests.request(method, base + path, timeout=90, **kwargs)
        assert res.status_code == expected, (
            method,
            path,
            res.status_code,
            res.text[:500],
        )
        return res

    os.environ["PAPERSEARCH_API"] = base
    app = AppTest.from_file(str(UI), default_timeout=90).run()
    assert not app.exception, app.exception
    topic = "Review " + uuid4().hex[:10]
    app.sidebar.text_input[0].set_value(topic)
    app.sidebar.text_area[0].set_value("Screen papers by their indexed abstract.")
    app.sidebar.button[0].click().run()
    assert not app.exception, app.exception
    project = next(
        p["project_id"] for p in http("GET", "/projects").json() if p["topic"] == topic
    )
    app.main.text_input[0].set_value(query).run()
    assert not app.exception and len(app.radio) == 1, app.exception
    prefix = f"/projects/{project}"
    page = http("GET", prefix + "/search", params=dict(q=query, limit=1)).json()
    hit = page["results"][0]
    paper, event = hit["paper_id"], hit["event_id"]
    detail = http(
        "GET", "/papers/detail", params=dict(paper=paper, event=event)
    ).json()["record"]
    assert detail["payload"]["source_url"] and detail["payload"]["abstract"]
    assert app.radio[0].value == event
    select = next(widget for widget in app.selectbox if widget.label == "Label")
    select.set_value("relevant")
    note = 'Retain this paper, "reviewed" offline.'
    next(w for w in app.text_area if w.label == "Review note").set_value(note)
    next(w for w in app.button if w.label == "Save decision").click().run()
    assert not app.exception, app.exception
    args = dict(paper=paper)

    def saved():
        return http("GET", prefix + "/decision", params=args).json()

    initial = saved()
    assert len(initial["history"]) == 1 and initial["current"]["note"] == note
    assert initial["current"]["reviewed_text_hash"] == detail["embedding_text_hash"]
    print("PASS Streamlit create/search/inspect/save", paper, event, flush=True)
    restart()
    assert saved() == initial
    edit = dict(
        reviewed_event_id=event,
        label="uncertain",
        note=note + " Recheck.",
        request_id=str(uuid4()),
    )

    def send(_):
        return http("PUT", prefix + "/decision", params=args, json=edit).json()[
            "history_id"
        ]

    with ThreadPoolExecutor(4) as pool:
        ids = set(pool.map(send, range(4)))
    assert len(ids) == 1 and len(saved()["history"]) == 2
    http(
        "PUT",
        prefix + "/decision",
        params=args,
        json={**edit, "note": "collision"},
        expected=409,
    )
    http(
        "PUT",
        prefix + "/decision",
        params=args,
        json={**edit, "label": "maybe"},
        expected=422,
    )
    http(
        "PUT",
        prefix + "/decision",
        params=args,
        json={**edit, "reviewed_event_id": "0" * 64, "request_id": str(uuid4())},
        expected=409,
    )
    http("GET", "/projects/bad/decision", params=args, expected=422)
    http("GET", "/papers/detail", params=dict(paper="missing"), expected=404)
    other = http(
        "POST", "/projects", expected=201, json=dict(topic="Independent " + topic)
    ).json()["project_id"]
    assert (
        http("GET", f"/projects/{other}/decision", params=args).json()["current"]
        is None
    )

    def concurrent_edit(number):
        payload = {
            **edit,
            "note": f"Concurrent edit {number}",
            "request_id": str(uuid4()),
        }
        return http(
            "PUT", f"/projects/{other}/decision", params=args, json=payload
        ).json()["history_id"]

    with ThreadPoolExecutor(4) as pool:
        distinct_ids = set(pool.map(concurrent_edit, range(4)))
    separate = http("GET", f"/projects/{other}/decision", params=args).json()
    assert len(distinct_ids) == len(separate["history"]) == 4
    assert separate["current"]["history_id"] == max(distinct_ids)
    assert len(saved()["history"]) == 2
    http("GET", prefix + "/search", params=dict(q="!!!"), expected=422)
    if page["has_more"]:
        next_page = http(
            "GET",
            prefix + "/search",
            params=dict(q=query, snapshot=page["snapshot_id"], limit=1, offset=1),
        ).json()
        assert (
            next_page["results"][0]["rank"] == 2
            and next_page["results"][0]["paper_id"] != paper
        )
    print(
        "PASS restart, concurrent idempotent retries, request collision, invalid inputs and project isolation",
        flush=True,
    )
    if revise:
        revise(detail)
        lagging = http("GET", "/status").json()
        assert lagging["metadata_generations_behind"] == 1
        assert (
            saved()["current"]["needs_reconsideration"] and len(saved()["history"]) == 2
        )
        assert (
            http("GET", "/papers/detail", params=dict(paper=paper, event=event)).json()[
                "record"
            ]
            == detail
        )
        app.run()
        warnings = [w.value for w in app.warning]
        assert any("behind" in w for w in warnings) and any(
            "Reconsider" in w for w in warnings
        )
        rebuild()
        http(
            "GET",
            prefix + "/search",
            params=dict(q=query, snapshot=page["snapshot_id"]),
            expected=409,
        )
        print(
            "PASS newer metadata, older indexed text, reconsideration, history and snapshot rollover",
            flush=True,
        )
    link = next(
        e for e in app.get("link_button") if e.proto.label == "Export decisions (CSV)"
    )
    assert link.proto.url == base + prefix + "/export.csv"
    export = http("GET", prefix + "/export.csv").content
    rows = list(csv.DictReader(io.StringIO(export.decode())))
    assert (
        len(rows) == 1
        and rows[0]["note"] == edit["note"]
        and rows[0]["reviewed_event_id"] == event
    )
    assert rows[0]["needs_reconsideration"] == str(int(bool(revise)))
    (output / "decisions.csv").write_bytes(export)
    print("PASS CSV export", output / "decisions.csv", flush=True)
    return project


def wait_http(url):
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            if requests.get(url, timeout=(3, 10)).status_code == 200:
                return
        except (requests.ConnectionError, requests.Timeout):
            pass
        time.sleep(1)
    raise RuntimeError(f"Service did not become ready: {url}")


def demo(keep_running):
    project = "papersearch-review-" + uuid4().hex[:10]
    folder = REPO / "artifacts" / project
    folder.mkdir(parents=True)
    env = {
        **os.environ,
        "ARTIFACT_DIR": str(folder),
        "REVIEW_API_PORT": os.environ.get("DEMO_API_PORT", "18000"),
        "REVIEW_UI_PORT": os.environ.get("DEMO_UI_PORT", "18501"),
        "PAPERSEARCH_TOPIC": "papers",
        "PAPERSEARCH_GROUP": "papersearch-ingest",
        "SNAPSHOT_ROOT": "/artifacts/derived",
    }
    compose = [
        "docker",
        "compose",
        "-p",
        project,
        "--profile",
        "core",
        "--profile",
        "snapshot",
        "--profile",
        "review",
    ]

    def run(*args, capture=False):
        return subprocess.run(
            compose + list(args),
            env=env,
            cwd=REPO,
            check=True,
            stdout=subprocess.PIPE if capture else None,
            text=True,
        ).stdout

    def cli(*args, capture=False):
        return run("run", "--rm", "--no-deps", "snapshot", *args, capture=capture)

    def ingest(file):
        archived = cli(
            "stream",
            "archive",
            "/artifacts/" + file,
            "--format",
            "fixture",
            "--batch-id",
            file,
            "--collected-at",
            "2020-02-01T00:00:00Z",
            capture=True,
        ).strip()
        cli("stream", "publish", "papers", archived)
        cli("stream", "consume", "papers", "--drain")

    def build():
        cli("snapshot", "build")

    def revise(detail):
        revised = dict(
            id="river",
            source_version=3,
            source_updated_at="2020-01-04T00:00:00Z",
            title=detail["payload"]["title"],
            abstract="Three streams after revision.",
        )
        (folder / "revision.jsonl").write_text(json.dumps(revised) + "\n")
        ingest("revision.jsonl")

    def url(service, port):
        return "http://" + run("port", service, str(port), capture=True).strip()

    def restart():
        run("restart", "mysql", "api", "ui")
        run("up", "-d", "--no-build", "--pull", "never", "--wait", "mysql")
        wait_http(base + "/ready")
        wait_http(browser + "/_stcore/health")

    subprocess.run(
        [
            sys.executable,
            "-m",
            "papersearch.cli",
            "preflight",
        ],
        cwd=REPO,
        env=env,
        check=True,
    )
    try:
        run("up", "-d", "--no-build", "--pull", "never", "--wait", "mysql", "kafka")
        run(
            "run",
            "--rm",
            "--pull",
            "never",
            "--entrypoint",
            "alembic",
            "snapshot",
            "upgrade",
            "head",
        )
        cli("stream", "init", "papers")
        line = (REPO / "tests/fixtures/phase1/cases.jsonl").read_bytes().splitlines()[0]
        (folder / "seed.jsonl").write_bytes(line + b"\n")
        ingest("seed.jsonl")
        build()
        run("up", "-d", "--no-build", "--pull", "never", "api", "ui")
        base, browser = url("api", 8000), url("ui", 8501)
        wait_http(base + "/ready")
        wait_http(browser + "/_stcore/health")
        workflow(base, folder, restart, revise, build)
        print("PASS offline reviewer demo", project, browser, flush=True)
    finally:
        if not keep_running:
            run("stop")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep-running", action="store_true")
    demo(parser.parse_args().keep_running)
