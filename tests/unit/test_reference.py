# AI-generated tests (Codex): expected records, replay, ordering, and bounded invalid input.
import hashlib
import json
import random
import subprocess
import sys
from pathlib import Path

import pytest

from papersearch.adapters import MAX_LINE_BYTES, json_record, parse_record
from papersearch.contracts import RecordError, arxiv_id, parse_envelope
from papersearch.reference import process_files

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "phase1"
EXPECTED = json.loads((FIXTURES / "expected.json").read_bytes())
CASES = FIXTURES / "cases.jsonl"
AT = "2020-02-01T00:00:00Z"


def run(paths, fmt="fixture"):
    return process_files(paths, fmt, "archive", AT)


def event(row, fmt="fixture"):
    data = json.dumps(row).encode()
    return parse_record(
        row,
        fmt,
        batch_id="archive",
        collected_at=AT,
        location="archive.jsonl#1",
        checksum=hashlib.sha256(data).hexdigest(),
    )


def assert_expected(result, name, copies=1):
    expected = EXPECTED[name]
    for key in ("state_sha256", "current_count", "unique_events"):
        assert result[key] == expected[key]
    assert result["rejections"] == EXPECTED["rejections"]
    assert result["conflicts"] == EXPECTED["conflicts"]
    assert result["records"] == expected["records"] * copies
    assert result["rejected"] == expected["rejected"] * copies
    assert (
        result["duplicates"]
        == result["records"] - result["rejected"] - expected["unique_events"]
    )
    assert {k: result["papers"][k] for k in EXPECTED["edge_papers"]} == EXPECTED[
        "edge_papers"
    ]
    assert "fixture:withheld" not in result["papers"]


def test_curated_oracle():
    assert (
        hashlib.sha256(CASES.read_bytes()).hexdigest()
        == EXPECTED["cases"]["input_sha256"]
    )
    assert_expected(run([CASES]), "cases")


def test_full_fixture_shuffles_and_two_replays(tmp_path):
    path = tmp_path / "development.jsonl"
    subprocess.run([sys.executable, FIXTURES / "generate.py", path], check=True)
    data = path.read_bytes()
    assert hashlib.sha256(data).hexdigest() == EXPECTED["development"]["input_sha256"]
    assert_expected(run([path]), "development")
    for seed in (7, 31, 544):
        lines = data.splitlines(keepends=True)
        random.Random(seed).shuffle(lines)
        shuffled = tmp_path / f"order-{seed}.jsonl"
        shuffled.write_bytes(b"".join(lines))
        assert_expected(run([shuffled]), "development")
        assert_expected(run([path, shuffled, path]), "development", copies=3)


def test_text_and_event_identity():
    rows = CASES.read_bytes().splitlines()
    original, metadata, changed = [event(json_record(rows[i])) for i in (0, 3, 4)]
    a, b, c = (e.canonical() for e in (original, metadata, changed))
    assert (
        a["embedding_text_hash"] == b["embedding_text_hash"] != c["embedding_text_hash"]
    )
    assert a["payload_hash"] != b["payload_hash"]
    row = json_record(rows[3])
    row["authors"].reverse()
    assert event(row).canonical()["payload_hash"] != b["payload_hash"]
    assert (
        event(json_record(rows[5])).canonical()
        == event(json_record(rows[6])).canonical()
    )


def test_envelope_round_trip_and_provenance(tmp_path):
    ev = event(json_record(CASES.read_bytes().splitlines()[0]))
    wire = ev.envelope()
    assert parse_envelope(wire) == ev
    location = "source  archive.jsonl#1"
    assert parse_envelope(dict(wire, raw_location=location)).raw_location == location
    missing = wire["payload"].copy()
    del missing["categories"]
    with pytest.raises(RecordError, match="invalid_envelope"):
        parse_envelope(dict(wire, payload=missing))
    with pytest.raises(RecordError, match="hash_mismatch"):
        parse_envelope(dict(wire, payload=dict(wire["payload"], title="Altered")))
    recollected = dict(
        wire,
        collected_at="2024-03-01T00:00:00Z",
        batch_id="different",
        raw_location="another.jsonl#7",
        raw_sha256="a" * 64,
    )
    path = tmp_path / "events.jsonl"
    path.write_text(
        "\n".join(
            json.dumps(x)
            for x in [
                wire,
                recollected,
                dict(wire, schema_version=2),
                dict(wire, event_id="0" * 64),
            ]
        ),
        encoding="utf-8",
    )
    result = run([path], "event")
    assert (result["duplicates"], result["unique_events"], result["rejected"]) == (
        1,
        1,
        2,
    )
    assert set(result["rejections"].values()) == {"unsupported_schema", "hash_mismatch"}
    assert result["papers"][ev.paper_id] == ev.canonical()


def test_tie_falls_back_then_newer_revision_recovers(tmp_path):
    base = dict(id="test", title="Original", source_updated_at=AT, source_version=1)
    rows = [
        base,
        dict(base, source_version=2),
        dict(base, source_version=2, title="Conflicting"),
        dict(
            base,
            title="Later revision",
            source_version=3,
            source_updated_at="2019-01-01T00:00:00Z",
        ),
    ]
    path = tmp_path / "tie.jsonl"
    for order in (rows, list(reversed(rows))):
        path.write_text("\n".join(map(json.dumps, order)), encoding="utf-8")
        result = run([path])
        assert result["papers"]["fixture:test"]["source_version"] == 3
        assert len(result["conflicts"]) == 1
    rows = [
        dict(base, source_version=None),
        dict(base, source_updated_at="2019-01-01T00:00:00Z"),
        dict(base, source_updated_at="2019-01-02T00:00:00Z", title="Newest known v1"),
    ]
    path.write_text("\n".join(map(json.dumps, reversed(rows))), encoding="utf-8")
    winner = run([path])["papers"]["fixture:test"]
    assert winner["payload"]["title"] == "Newest known v1"


def test_oversized_and_malformed_lines_do_not_hide_next_record(tmp_path):
    oversize = b"x" * (2 * MAX_LINE_BYTES + 5) + b"\n"
    raw = [
        oversize,
        b'{"x":1,"x":2}\n',
        b"\xff\n",
        b"[]\n",
        b'{"x":NaN}\n',
        b'{"id":"a","source_updated_at":"2020-01-01T00:00:00Z","title":"\\ud800"}\n',
    ]
    path = tmp_path / "mixed.jsonl"
    path.write_bytes(b"".join(raw) + CASES.read_bytes().splitlines(keepends=True)[7])
    result = run([path])
    assert result["rejections"] == {
        hashlib.sha256(line).hexdigest(): code
        for line, code in zip(
            raw,
            [
                "record_too_large",
                "duplicate_key",
                "invalid_json",
                "invalid_record",
                "invalid_json",
                "invalid_text",
            ],
            strict=True,
        )
    }
    assert result["current_count"] == 1
    with pytest.raises(FileNotFoundError):
        run([tmp_path / "absent"])


def test_snapshot_adapter():
    row = json_record((FIXTURES / "arxiv-metadata.jsonl").read_bytes())
    record = event(row, "arxiv")
    assert record.paper_id == "arxiv:0704.0001"
    assert record.source_version == 2
    assert record.source_updated_at == "2008-11-26T00:00:00.000000Z"
    assert record.payload.published == "2007-04-02"
    assert record.payload.authors[0] == "C. Balázs"
    assert record.payload.source_url == "https://arxiv.org/abs/0704.0001"
    assert (
        record.canonical()
        == event(dict(row, id="arXiv:0704.0001v2"), "arxiv").canonical()
    )
    history = list(reversed(row["versions"]))
    fallback = event(
        dict(row, versions=history, update_date=None, authors_parsed=None), "arxiv"
    )
    assert fallback.source_updated_at == "2007-07-24T20:10:27.000000Z"
    assert fallback.payload.authors == (row["authors"],)
    assert event(dict(row, versions=[]), "arxiv").source_version is None
    for changes, reason in [
        ({"id": "0704.0001v1"}, "version_mismatch"),
        ({"versions": [], "update_date": None}, "missing_source_time"),
        ({"versions": history * 2}, "invalid_history"),
    ]:
        with pytest.raises(RecordError, match=reason):
            event(dict(row, **changes), "arxiv")


@pytest.mark.parametrize(
    ("value", "base", "revision"),
    [
        ("hep-th/9901001v10", "arxiv:hep-th/9901001", 10),
        ("math.GT/0309136", "arxiv:math/0309136", None),
        ("arXiv:1501.00001v1", "arxiv:1501.00001", 1),
    ],
)
def test_arxiv_identifiers(value, base, revision):
    assert arxiv_id(value) == (base, revision)


@pytest.mark.parametrize(
    "value", ["../0704.0001", "0713.0001", "1501.0001", "0704.0001v0", "0704.0000"]
)
def test_invalid_arxiv_identifier(value):
    with pytest.raises(RecordError, match="invalid_paper_id"):
        arxiv_id(value)
