# AI-generated code (Codex): interactive review of published search results through the HTTP API.
import os
from uuid import uuid4

import requests
import streamlit as st

BASE = os.environ.get("PAPERSEARCH_API", "http://127.0.0.1:8000")
st.set_page_config(page_title="PaperSearch", layout="wide")
st.title("PaperSearch")


def call(method, path, **kwargs):
    try:
        response = requests.request(method, BASE + path, timeout=(4, 90), **kwargs)
        if response.status_code == 409:
            st.warning(response.json()["detail"])
            if "search" in path and st.button("Restart search"):
                st.session_state.pop("page", None)
                st.rerun()
            st.stop()
        response.raise_for_status()
    except requests.RequestException as exc:
        st.error(f"The API request failed: {exc}")
        st.stop()
    return response


with st.sidebar:
    st.header("Review projects")
    with st.form("project"):
        topic = st.text_input("New project topic")
        criteria = st.text_area("Screening criteria")
        if st.form_submit_button("Create project"):
            new = call(
                "POST", "/projects", json=dict(topic=topic, criteria=criteria)
            ).json()
            st.session_state.project_id = new["project_id"]
            st.rerun()
    projects = {p["project_id"]: p for p in call("GET", "/projects").json()}
    if projects:
        project = st.selectbox(
            "Current project",
            list(projects),
            format_func=lambda p: projects[p]["topic"],
            key="project_id",
        )
        st.write(projects[project]["criteria"])
        public_api = os.environ.get("PAPERSEARCH_PUBLIC_API", BASE)
        st.link_button(
            "Export decisions (CSV)", f"{public_api}/projects/{project}/export.csv"
        )

with st.expander("Processing status", expanded=False):
    stats = call("GET", "/status").json()
    active = stats["active"]
    st.write("Current papers", stats["input"]["current_revisions"])
    if active:
        coverage = active["coverage"]
        st.write(
            f"Index generation {active['generation']} · Metadata generation {stats['corpus_generation']} · "
            f"{coverage['lexical']:,} lexical · {coverage['semantic']:,} semantic"
        )
    st.write("Kafka", stats["broker"])
    st.write("Durable inputs", stats["input"])
    st.write("Derived jobs", stats["jobs"])
    if stats["recent_errors"]:
        st.write(stats["recent_errors"])
if not active:
    st.info("Publish a search snapshot to begin screening.")
    st.stop()
if stats["metadata_generations_behind"]:
    st.warning(
        f"Search is {stats['metadata_generations_behind']} metadata generations behind. "
        "Results show indexed revisions."
    )
if not projects:
    st.info("Create a review project to begin.")
    st.stop()

prefix = f"/projects/{project}"
q = st.text_input("Search papers", placeholder="Enter title or abstract terms")
field = st.selectbox("Search in", ["all", "title", "abstract"])
if not q.strip():
    st.stop()
identity = (project, q, field)
if st.session_state.get("search_key") != identity:
    st.session_state.search_key = identity
    st.session_state.pop("page", None)
offset, snapshot = st.session_state.get("page", (0, None))
params = dict(q=q, field=field, offset=offset, limit=10, snapshot=snapshot)
page = call("GET", prefix + "/search", params=params).json()
hits = page["results"]
if not hits:
    st.info("No papers matched these terms.")
    st.stop()
previous, following = st.columns(2)
if previous.button("Previous page", disabled=offset == 0):
    st.session_state.page = (offset - 10, page["snapshot_id"])
    st.rerun()
if following.button("Next page", disabled=not page["has_more"]):
    st.session_state.page = (offset + 10, page["snapshot_id"])
    st.rerun()
choices = {r["event_id"]: r for r in hits}
chosen = st.radio(
    "Ranked papers",
    list(choices),
    format_func=lambda i: f"{choices[i]['rank']}. {choices[i]['title']}",
)
paper = choices[chosen]["paper_id"]
detail = call("GET", "/papers/detail", params=dict(paper=paper, event=chosen)).json()
event = st.selectbox(
    "Inspect revision",
    [r["event_id"] for r in detail["revisions"]],
    index=[r["event_id"] for r in detail["revisions"]].index(chosen),
    format_func=lambda e: next(
        f"v{r['version_rank']} · {r['source_updated_at']} · {e[:10]}"
        for r in detail["revisions"]
        if r["event_id"] == e
    ),
)
if event != chosen:
    detail = call("GET", "/papers/detail", params=dict(paper=paper, event=event)).json()
record = detail["record"]
payload = record["payload"]
st.subheader(payload["title"])
st.caption(paper + " · " + ", ".join(payload["categories"]))
st.write(payload["abstract"])
if payload["source_url"]:
    st.link_button("Open source", payload["source_url"])
if detail["current_event_id"] != event:
    st.warning("You are viewing an older or superseded revision.")
review = call("GET", prefix + "/decision", params=dict(paper=paper)).json()
current = review["current"] or {}
if current.get("needs_reconsideration"):
    st.warning("The reviewed text has changed. Reconsider this decision.")
labels = ["relevant", "irrelevant", "uncertain"]
form_id = f"{project}-{event}-{current.get('history_id', 0)}"
with st.form(form_id):
    label = st.selectbox(
        "Label",
        labels,
        index=labels.index(current.get("label", "uncertain")),
        key="label-" + form_id,
    )
    note = st.text_area("Review note", current.get("note", ""), key="note-" + form_id)
    if st.form_submit_button("Save decision"):
        identity = (project, paper, event, label, note)
        if st.session_state.get("submission") != identity:
            st.session_state.submission = identity
            st.session_state.request_id = str(uuid4())
        call(
            "PUT",
            prefix + "/decision",
            params=dict(paper=paper),
            json=dict(
                reviewed_event_id=event,
                request_id=st.session_state.request_id,
                label=label,
                note=note,
            ),
        )
        del st.session_state.submission
        st.rerun()
if review["history"]:
    with st.expander("Decision history"):
        st.table(review["history"])
