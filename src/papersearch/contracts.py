# AI-generated code (Codex): normalized paper records and the v1 event envelope.
import hashlib
import json
import re
import unicodedata
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from urllib.parse import urlsplit


class RecordError(ValueError):
    """Permanent validation failure. The message is the stable rejection code."""


def digest(value: object) -> str:
    data = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def clean(value: object, limit: int, *, empty: bool = False) -> str:
    if not isinstance(value, str) or len(value) > limit:
        raise RecordError("invalid_text")
    if any(
        unicodedata.category(c) in {"Cc", "Cs"} and c not in "\t\r\n" for c in value
    ):
        raise RecordError("invalid_text")
    result = " ".join(unicodedata.normalize("NFC", value).split())
    if not result and not empty:
        raise RecordError("invalid_text")
    return result


def provenance(value: object, limit: int) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > limit:
        raise RecordError("invalid_provenance")
    if any(unicodedata.category(c) in {"Cc", "Cs"} for c in value):
        raise RecordError("invalid_provenance")
    return value


def utc(value: object) -> str:
    if not isinstance(value, str) or not re.fullmatch(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?(?:Z|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])",
        value,
    ):
        raise RecordError("invalid_timestamp")
    try:
        dt = datetime.fromisoformat(value).astimezone(timezone.utc)
    except (ValueError, OverflowError) as exc:
        raise RecordError("invalid_timestamp") from exc
    return dt.isoformat(timespec="microseconds").replace("+00:00", "Z")


def iso_day(value: object) -> str:
    if not isinstance(value, str) or not re.fullmatch(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value
    ):
        raise RecordError("invalid_date")
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise RecordError("invalid_date") from exc


def version(value: object) -> int | None:
    if value is not None and (type(value) is not int or not 1 <= value <= 2**31 - 1):
        raise RecordError("invalid_version")
    return value


def arxiv_id(value: object) -> tuple[str, int | None]:
    if not isinstance(value, str) or len(value) > 100:
        raise RecordError("invalid_paper_id")
    identifier = re.sub(r"^arxiv:", "", value, count=1, flags=re.IGNORECASE)
    match = re.fullmatch(
        r"(?:(?P<modern>[0-9]{2}(?:0[1-9]|1[0-2])\.[0-9]{4,5})|"
        r"(?P<archive>[a-z][a-z0-9-]*)(?:\.[A-Za-z]{2})?/(?P<old>[0-9]{2}(?:0[1-9]|1[0-2])[0-9]{3}))"
        r"(?:v(?P<v>[1-9][0-9]{0,9}))?",
        identifier,
    )
    if not match:
        raise RecordError("invalid_paper_id")
    if match["modern"]:
        month, serial = match["modern"].split(".")
        if (
            "0701" <= month <= "0703"
            or len(serial) != (4 if "0704" <= month <= "1412" else 5)
            or int(serial) == 0
        ):
            raise RecordError("invalid_paper_id")
    else:
        month, serial = match["old"][:4], match["old"][4:]
        if "0703" < month < "9107" or int(serial) == 0:
            raise RecordError("invalid_paper_id")
    base = match["modern"] or f"{match['archive']}/{match['old']}"
    return "arxiv:" + base, version(int(match["v"]) if match["v"] else None)


def strings(value: object, limit: int) -> list[str]:
    if not isinstance(value, list) or len(value) > limit:
        raise RecordError("invalid_list")
    return [clean(item, 1024) for item in value]


def validate_url(value: object) -> str:
    if (
        not isinstance(value, str)
        or not value.isascii()
        or len(value) > 2048
        or re.search(r"[\s\x00-\x1f\x7f\\]", value)
    ):
        raise RecordError("invalid_url")
    try:
        parts = urlsplit(value)
        if (
            parts.scheme not in {"http", "https"}
            or not parts.hostname
            or parts.username is not None
        ):
            raise RecordError("invalid_url")
        _ = parts.port
    except ValueError as exc:
        raise RecordError("invalid_url") from exc
    return value


@dataclass(frozen=True)
class Payload:
    title: str
    abstract: str
    authors: tuple[str, ...]
    categories: tuple[str, ...]
    published: str | None
    source_url: str
    doi: str | None

    @classmethod
    def parse(cls, row: dict) -> "Payload":
        return cls(
            title=clean(row.get("title"), 4096),
            abstract=clean(row.get("abstract", ""), 65536, empty=True),
            authors=tuple(strings(row.get("authors", []), 4096)),
            categories=tuple(sorted(set(strings(row.get("categories", []), 256)))),
            published=iso_day(row["published"])
            if row.get("published") is not None
            else None,
            source_url=validate_url(row.get("source_url")),
            doi=clean(row["doi"], 512) if row.get("doi") is not None else None,
        )


@dataclass(frozen=True)
class Event:
    source: str
    paper_id: str
    source_version: int | None
    source_updated_at: str
    collected_at: str
    batch_id: str
    raw_location: str
    raw_sha256: str
    payload: Payload

    def canonical(self) -> dict:
        metadata = asdict(self.payload)
        metadata.update(
            authors=list(self.payload.authors), categories=list(self.payload.categories)
        )
        payload_hash = digest(metadata)
        identity = [
            1,
            self.source,
            self.paper_id,
            self.source_version,
            self.source_updated_at,
            payload_hash,
        ]
        return dict(
            schema_version=1,
            source=self.source,
            paper_id=self.paper_id,
            source_version=self.source_version,
            source_updated_at=self.source_updated_at,
            payload=metadata,
            payload_hash=payload_hash,
            event_id=digest(identity),
            embedding_text_hash=digest(
                ["text-v1", self.payload.title, self.payload.abstract]
            ),
        )

    def envelope(self) -> dict:
        return dict(
            self.canonical(),
            collected_at=self.collected_at,
            batch_id=self.batch_id,
            raw_location=self.raw_location,
            raw_sha256=self.raw_sha256,
        )


def parse_envelope(row: dict) -> Event:
    if type(row.get("schema_version")) is not int or row["schema_version"] != 1:
        raise RecordError("unsupported_schema")
    source, paper = row.get("source"), row.get("paper_id")
    if source == "arxiv":
        if arxiv_id(paper) != (paper, None):
            raise RecordError("invalid_paper_id")
    elif source == "fixture":
        if not isinstance(paper, str) or not re.fullmatch(
            r"fixture:[a-z0-9][a-z0-9_-]{0,79}", paper
        ):
            raise RecordError("invalid_paper_id")
    else:
        raise RecordError("invalid_source")
    checksum = row.get("raw_sha256")
    if not isinstance(checksum, str) or not re.fullmatch("[0-9a-f]{64}", checksum):
        raise RecordError("invalid_provenance")
    required_payload = {
        "title",
        "abstract",
        "authors",
        "categories",
        "published",
        "source_url",
    }
    if (
        not isinstance(row.get("payload"), dict)
        or not required_payload.issubset(row["payload"])
        or "source_version" not in row
    ):
        raise RecordError("invalid_envelope")
    event = Event(
        source,
        paper,
        version(row["source_version"]),
        utc(row.get("source_updated_at")),
        utc(row.get("collected_at")),
        provenance(row.get("batch_id"), 256),
        provenance(row.get("raw_location"), 4096),
        checksum,
        Payload.parse(row["payload"]),
    )
    canonical = event.canonical()
    if any(
        row.get(key) != canonical[key]
        for key in ("event_id", "payload_hash", "embedding_text_hash")
    ):
        raise RecordError("hash_mismatch")
    return event
