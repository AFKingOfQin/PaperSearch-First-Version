# AI-generated code (Codex): sequential, in-memory revision oracle.
from pathlib import Path

from .adapters import json_record, parse_record, raw_lines
from .contracts import RecordError, digest, provenance, utc


def select_revision(paper, revisions):
    """Skip each disputed priority, retaining the newest unambiguous alternative."""
    winner = None
    conflicts = []
    for (number, updated), alternatives in sorted(revisions.items(), reverse=True):
        if len(alternatives) > 1:
            conflicts.append(
                dict(
                    reason="revision_conflict",
                    paper_id=paper,
                    source_version=number or None,
                    source_updated_at=updated,
                    event_ids=sorted(alternatives),
                )
            )
        elif winner is None:
            winner = next(iter(alternatives.values()))
    return winner, conflicts


def process_files(
    paths: list[Path], fmt: str, batch_id: str, collected_at: str
) -> dict:
    """Retain distinct revisions so a later conflict can retract an earlier candidate."""
    if fmt != "event":
        batch_id = provenance(batch_id, 256)
        collected_at = utc(collected_at)
    history = {}
    rejections = {}
    total = duplicates = rejected = 0
    for path in paths:
        for line, checksum, raw in raw_lines(path):
            total += 1
            try:
                event = parse_record(
                    json_record(raw),
                    fmt,
                    batch_id=batch_id,
                    collected_at=collected_at,
                    location=f"{path.as_posix()}#{line}",
                    checksum=checksum,
                )
            except RecordError as exc:
                rejections[checksum] = str(exc)
                rejected += 1
                continue
            priority = (event.source_version or 0, event.source_updated_at)
            alternatives = history.setdefault(event.paper_id, {}).setdefault(
                priority, {}
            )
            record = event.canonical()
            if record["event_id"] in alternatives:
                duplicates += 1
            else:
                alternatives[record["event_id"]] = record

    papers = {}
    conflicts = []
    for paper, revisions in sorted(history.items()):
        winner, disputed = select_revision(paper, revisions)
        if winner is not None:
            papers[paper] = winner
        conflicts.extend(disputed)
    return dict(
        records=total,
        duplicates=duplicates,
        rejected=rejected,
        unique_events=total - duplicates - rejected,
        current_count=len(papers),
        state_sha256=digest(papers),
        papers=papers,
        conflicts=conflicts,
        rejections=rejections,
    )
