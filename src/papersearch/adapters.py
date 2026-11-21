# AI-generated code (Codex): offline fixture and arXiv bulk JSONL adapters.
import hashlib
import json
import re
from datetime import timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

from .contracts import (
    Event,
    Payload,
    RecordError,
    arxiv_id,
    clean,
    iso_day,
    parse_envelope,
    provenance,
    utc,
    version,
)

MAX_LINE_BYTES = 256 * 1024


def raw_lines(path: Path):
    """Hash the exact archived bytes, including the line ending. Drain oversized lines in chunks."""
    with path.open("rb") as stream:
        number = 0
        while chunk := stream.readline(MAX_LINE_BYTES + 1):
            number += 1
            checksum = hashlib.sha256(chunk)
            if len(chunk) <= MAX_LINE_BYTES:
                yield number, checksum.hexdigest(), chunk
                continue
            while chunk and not chunk.endswith(b"\n"):
                chunk = stream.readline(MAX_LINE_BYTES)
                checksum.update(chunk)
            yield number, checksum.hexdigest(), None


def unique_keys(pairs: list) -> dict:
    obj = {}
    for key, value in pairs:
        if key in obj:
            raise RecordError("duplicate_key")
        obj[key] = value
    return obj


def invalid_constant(value: str):
    raise RecordError("invalid_json")


def json_record(raw: bytes | None) -> dict:
    if raw is None:
        raise RecordError("record_too_large")
    try:
        row = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=unique_keys,
            parse_constant=invalid_constant,
        )
    except RecordError:
        raise
    except (ValueError, RecursionError) as exc:
        raise RecordError("invalid_json") from exc
    if not isinstance(row, dict):
        raise RecordError("invalid_record")
    return row


def snapshot_record(row: dict) -> tuple:
    paper, suffix = arxiv_id(row.get("id"))
    history = row.get("versions", [])
    if not isinstance(history, list) or len(history) > 4096:
        raise RecordError("invalid_history")
    dates = {}
    for entry in history:
        if not isinstance(entry, dict):
            raise RecordError("invalid_history")
        label, created = entry.get("version"), entry.get("created")
        if not isinstance(label, str) or not re.fullmatch(r"v[1-9][0-9]{0,9}", label):
            raise RecordError("invalid_history")
        n = version(int(label[1:]))
        if n in dates or not isinstance(created, str) or len(created) > 64:
            raise RecordError("invalid_history")
        try:
            stamp = parsedate_to_datetime(created)
            if stamp.tzinfo is None:
                raise ValueError("missing timezone")
            dates[n] = utc(stamp.astimezone(timezone.utc).isoformat())
        except (ValueError, OverflowError) as exc:
            raise RecordError("invalid_history") from exc
    revision = max(dates, default=suffix)
    if suffix is not None and revision != suffix:
        raise RecordError("version_mismatch")
    times = [dates[revision]] if dates else []
    if row.get("update_date") is not None:
        times.append(utc(iso_day(row["update_date"]) + "T00:00:00Z"))
    if not times:
        raise RecordError("missing_source_time")
    authors = []
    parsed = row.get("authors_parsed")
    if parsed is not None and not isinstance(parsed, list):
        raise RecordError("invalid_authors")
    if parsed:
        for name in parsed:
            if not isinstance(name, list) or len(name) < 2:
                raise RecordError("invalid_authors")
            last, first, *suffixes = [
                clean(part, 1024, empty=True) for part in name[:3]
            ]
            authors.append(" ".join([first, last, *suffixes]).strip())
    elif row.get("authors") is not None:
        byline = clean(row["authors"], 1024, empty=True)
        authors = [byline] if byline else []
    categories = clean(row.get("categories", ""), 8192, empty=True).split()
    payload = Payload.parse(
        dict(
            title=row.get("title"),
            abstract=row.get("abstract", ""),
            authors=authors,
            categories=categories,
            doi=row.get("doi"),
            published=dates[1][:10] if 1 in dates else None,
            source_url="https://arxiv.org/abs/" + paper.removeprefix("arxiv:"),
        )
    )
    return paper, revision, max(times), payload


def parse_record(
    row: dict,
    fmt: str,
    *,
    batch_id: str,
    collected_at: str,
    location: str,
    checksum: str,
) -> Event:
    if fmt == "event":
        return parse_envelope(row)
    if fmt == "arxiv":
        paper, revision, updated, payload = snapshot_record(row)
    elif fmt == "fixture":
        schema = row.get("schema_version", 1)
        if type(schema) is not int or schema != 1:
            raise RecordError("unsupported_schema")
        identifier = row.get("id")
        if not isinstance(identifier, str) or not re.fullmatch(
            r"[a-z0-9][a-z0-9_-]{0,79}", identifier
        ):
            raise RecordError("invalid_paper_id")
        paper = "fixture:" + identifier
        revision = version(row.get("source_version"))
        updated = utc(row.get("source_updated_at"))
        payload = Payload.parse(
            {"source_url": "https://example.invalid/papers/" + identifier, **row}
        )
    else:
        raise ValueError(f"Unknown source format: {fmt}")
    return Event(
        fmt,
        paper,
        revision,
        updated,
        utc(collected_at),
        provenance(batch_id, 256),
        provenance(location, 4096),
        checksum,
        payload,
    )
