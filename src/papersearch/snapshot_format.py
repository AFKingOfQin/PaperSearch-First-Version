# AI-generated code (Codex): normalized Parquet schemas and cross-file snapshot validation.
import hashlib
import json
import sqlite3
import struct
from collections import Counter
from itertools import zip_longest

import pyarrow as pa
import pyarrow.parquet as pq

from .contracts import digest
from .encoder import (
    DIM,
    ELIGIBILITY,
    LEGACY_ELIGIBILITY,
    MAX_TOKENS,
    RECIPE,
    eligible,
    recent_cs,
    unpack_vector,
)
from .derivation import TEST_KIND, WIDTH, cache_key, fixture_vector

STR = pa.string()
PAYLOAD = (
    "title",
    "abstract",
    "authors",
    "categories",
    "published",
    "source_url",
    "doi",
)
CORPUS = pa.schema(
    [
        ("row_id", pa.int64()),
        ("paper_id", STR),
        ("event_id", STR),
        ("embedding_text_hash", STR),
        ("schema_version", pa.int32()),
        ("source", STR),
        ("source_version", pa.int64()),
        ("source_updated_at", STR),
        ("payload_hash", STR),
        ("title", STR),
        ("abstract", STR),
        ("authors", pa.list_(STR)),
        ("categories", pa.list_(STR)),
        ("published", STR),
        ("source_url", STR),
        ("doi", STR),
    ]
)
VECTORS = pa.schema(
    [
        ("vector_row", pa.int64()),
        ("row_id", pa.int64()),
        ("paper_id", STR),
        ("event_id", STR),
        ("text_hash", STR),
        ("cache_key", STR),
        ("value", pa.binary(WIDTH)),
    ]
)
SEMANTIC = pa.schema(
    [
        *list(VECTORS)[:-1],
        ("tokens", pa.uint32()),
        ("vector_sha", STR),
        ("value", pa.binary(DIM * 4)),
    ]
)
FILES = ("corpus.parquet", "lexical.sqlite", "test-vectors.parquet")


def file_sha(path):
    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


class CorpusDigest:
    """Incremental equivalent of the offline oracle's digest of a paper-ID mapping."""

    def __init__(self):
        self.hash = hashlib.sha256(b"{")
        self.count = 0

    def add(self, record):
        if self.count:
            self.hash.update(b",")
        self.hash.update(json.dumps(record["paper_id"]).encode() + b":")
        self.hash.update(
            json.dumps(
                record, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode()
        )
        self.count += 1

    def result(self):
        h = self.hash.copy()
        h.update(b"}")
        return h.hexdigest()


def flat_row(number, record):
    return dict(
        row_id=number,
        **{k: v for k, v in record.items() if k != "payload"},
        **record["payload"],
    )


def include_date(bounds, day):
    if not day:
        return bounds
    if bounds is None:
        return [day, day]
    return [min(bounds[0], day), max(bounds[1], day)]


class SemanticScope:
    """Count current papers and actual vector mappings, including cross-listed categories."""

    def __init__(self, cohorts):
        self.recent_candidates = 0
        self.recent_indexed = 0
        self.categories = Counter()
        self.years = Counter()
        self.recent_dates = None
        self.other_dates = None
        self.other_indexed = 0
        self.cohorts = [
            dict(
                start=c["start"],
                before=c["before"],
                categories=c["categories"],
                eligible=0,
                indexed=0,
            )
            for c in cohorts
        ]

    def add(self, payload, indexed):
        day = payload["published"]
        if recent_cs(payload):
            self.recent_candidates += 1
            for cohort in self.cohorts:
                if (
                    day >= cohort["start"]
                    and (not cohort["before"] or day < cohort["before"])
                    and all(c in payload["categories"] for c in cohort["categories"])
                ):
                    cohort["eligible"] += 1
                    cohort["indexed"] += int(indexed)
            if indexed:
                self.recent_indexed += 1
                self.categories.update(set(payload["categories"]))
                self.years[day[:4]] += 1
                self.recent_dates = include_date(self.recent_dates, day)
        elif indexed:
            self.other_indexed += 1
            self.other_dates = include_date(self.other_dates, day)

    def summary(self):
        return dict(
            recent_eligible=self.recent_candidates,
            recent_indexed=self.recent_indexed,
            recent_dates=self.recent_dates,
            recent_categories=dict(self.categories),
            recent_years=dict(self.years),
            other_indexed=self.other_indexed,
            other_dates=self.other_dates,
            cohorts=self.cohorts,
        )


def parquet_rows(path, schema):
    with pq.ParquetFile(path) as parquet:
        if parquet.schema_arrow != schema:
            raise ValueError(f"Unexpected Parquet schema in {path.name}")
        for batch in parquet.iter_batches(batch_size=32, use_threads=False):
            yield from batch.to_pylist()


def validate(directory, expected_sha):
    path = directory / "manifest.json"
    if file_sha(path) != expected_sha:
        raise ValueError("Snapshot manifest checksum mismatch")
    manifest = json.loads(path.read_bytes())
    version = manifest["format_version"]
    semantic = version == 2
    files = FILES + (("semantic.parquet",) if semantic else ())
    if version not in (1, 2) or set(manifest["files"]) != set(files):
        raise ValueError("Unexpected snapshot package format")
    for name in files:
        f = directory / name
        if manifest["files"][name] != dict(bytes=f.stat().st_size, sha256=file_sha(f)):
            raise ValueError(f"Snapshot checksum mismatch: {name}")
    corpus = CorpusDigest()
    recipe = manifest["recipe"]
    vectors = parquet_rows(directory / "test-vectors.parquet", VECTORS)
    vector = next(vectors, None)
    indexed = fixtures = semantic_count = candidate_count = truncated = title_only = 0
    semantic_dates = None
    scope = SemanticScope(manifest.get("semantic_scope", {}).get("cohorts", []))
    semantic_rows = (
        parquet_rows(directory / "semantic.parquet", SEMANTIC) if semantic else iter(())
    )
    entry = next(semantic_rows, None)
    if semantic and (
        recipe != RECIPE
        or manifest["eligibility"] not in (LEGACY_ELIGIBILITY, ELIGIBILITY)
    ):
        raise ValueError("Unsupported semantic package recipe or scope")
    previous_id = ""
    db = sqlite3.connect(
        (directory / "lexical.sqlite").resolve().as_uri() + "?mode=ro&immutable=1",
        uri=True,
    )
    try:
        lexical = db.execute(
            "SELECT rowid,paper_id,event_id,text_hash,title,abstract FROM documents ORDER BY rowid"
        )
        for row, document in zip_longest(
            parquet_rows(directory / "corpus.parquet", CORPUS), lexical
        ):
            if row is None or document is None:
                raise ValueError("Lexical index and corpus lengths differ")
            number = row.pop("row_id")
            payload = {k: row.pop(k) for k in PAYLOAD}
            row["payload"] = payload
            paper, event, text_hash = (
                row["paper_id"],
                row["event_id"],
                row["embedding_text_hash"],
            )
            if (
                paper <= previous_id
                or number != corpus.count + 1
                or document
                != (
                    number,
                    paper,
                    event,
                    text_hash,
                    payload["title"],
                    payload["abstract"],
                )
            ):
                raise ValueError("Invalid lexical row-to-paper mapping")
            if (
                row["payload_hash"] != digest(payload)
                or text_hash
                != digest(["text-v1", payload["title"], payload["abstract"]])
                or event
                != digest(
                    [
                        row["schema_version"],
                        row["source"],
                        paper,
                        row["source_version"],
                        row["source_updated_at"],
                        row["payload_hash"],
                    ]
                )
            ):
                raise ValueError("Invalid canonical record identity")
            previous_id = paper
            corpus.add(row)
            fixtures += paper.startswith("fixture:")
            new_policy = semantic and manifest["eligibility"] == ELIGIBILITY
            candidate = semantic and eligible(paper, payload if new_policy else None)
            mapped = entry is not None and entry["row_id"] == number
            if new_policy:
                scope.add(payload, mapped)
            if candidate:
                candidate_count += 1
            if mapped:
                raw = struct.pack("<I", entry["tokens"]) + entry["value"]
                length = unpack_vector(raw)
                if (
                    not candidate
                    or entry["vector_row"] != semantic_count
                    or (entry["paper_id"], entry["event_id"], entry["text_hash"])
                    != (paper, event, text_hash)
                    or entry["cache_key"] != cache_key(text_hash, recipe)
                    or hashlib.sha256(raw).hexdigest() != entry["vector_sha"]
                ):
                    raise ValueError("Invalid semantic vector identity or row mapping")
                semantic_count += 1
                semantic_dates = include_date(semantic_dates, payload["published"])
                truncated += length > MAX_TOKENS
                title_only += not payload["abstract"]
                entry = next(semantic_rows, None)
            if vector is not None and vector["row_id"] == number:
                key = cache_key(text_hash, recipe)
                if (
                    recipe["kind"] != TEST_KIND
                    or not paper.startswith("fixture:")
                    or vector
                    != dict(
                        vector_row=indexed,
                        row_id=number,
                        paper_id=paper,
                        event_id=event,
                        text_hash=text_hash,
                        cache_key=key,
                        value=fixture_vector(key),
                    )
                ):
                    raise ValueError("Invalid test-vector identity or row mapping")
                indexed += 1
                vector = next(vectors, None)
    finally:
        db.close()
        vectors.close()
        if semantic:
            semantic_rows.close()
    if (
        "semantic_submission_dates" in manifest
        and manifest["semantic_submission_dates"] != semantic_dates
    ):
        raise ValueError("Semantic submission dates do not match published vectors")
    counts = dict(
        metadata=corpus.count,
        lexical=corpus.count,
        semantic=semantic_count,
        fixture_eligible=fixtures,
        test_vectors=indexed,
    )
    if semantic:
        if (
            manifest["eligibility"] == ELIGIBILITY
            and manifest.get("semantic_scope") != scope.summary()
        ):
            raise ValueError("Semantic scope does not match published mappings")
        counts.update(
            semantic_eligible=candidate_count,
            semantic_truncated=truncated,
            semantic_title_only=title_only,
        )
    if (
        entry is not None
        or vector is not None
        or counts != manifest["coverage"]
        or corpus.result() != manifest["corpus_sha256"]
    ):
        raise ValueError("Snapshot rows do not match manifest")
    return manifest
