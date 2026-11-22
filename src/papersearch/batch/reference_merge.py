# AI-generated code (Codex): bounded, sequential batch comparison using the project's revision oracle.
import heapq
from itertools import chain, count, groupby, islice
from operator import itemgetter
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from papersearch.reference import select_revision
from papersearch.snapshot_format import CORPUS

HISTORY = CORPUS.remove(0)
MERGED = HISTORY.append(pa.field("previous_event_id", pa.string())).append(
    pa.field("change", pa.string())
)


def parquet_rows(path):
    with pq.ParquetFile(path) as file:
        for batch in file.iter_batches(batch_size=128, use_threads=False):
            yield from batch.to_pylist()


def sorted_rows(directory, reader=parquet_rows, pattern="*.parquet"):
    # Inputs and outputs are sorted within each part; merge the ordered streams.
    files = sorted(Path(directory).glob(pattern))
    if not files:
        raise ValueError(f"No {pattern} parts at {directory}")
    return heapq.merge(*(reader(f) for f in files), key=itemgetter("paper_id"))


def merged_reference(base, updates):
    def tagged(folder, is_base):
        for record in sorted_rows(folder):
            yield record["paper_id"], is_base, record

    stream = heapq.merge(tagged(base, True), tagged(updates, False), key=itemgetter(0))
    for paper, group in groupby(stream, key=itemgetter(0)):
        old, all_events = {}, {}
        for _, is_base, row in group:
            row["categories"] = sorted(set(row["categories"]))
            priority = (row["source_version"] or 0, row["source_updated_at"])
            all_events.setdefault(priority, {})[row["event_id"]] = row
            if is_base:
                old.setdefault(priority, {})[row["event_id"]] = row
        previous, _ = select_revision(paper, old)
        current, _ = select_revision(paper, all_events)
        if previous is None and current is None:
            continue
        prior_id = previous["event_id"] if previous else None
        if current is None:
            current = {**dict.fromkeys(HISTORY.names), "paper_id": paper}
            change = "withdrawn"
        elif previous is None:
            change = "new"
        elif current["event_id"] == prior_id:
            change = "unchanged"
        else:
            change = "changed"
        yield dict(current, previous_event_id=prior_id, change=change)


def write_reference(base, updates, directory):
    directory.mkdir()
    rows = merged_reference(base, updates)
    blocks = iter(lambda: list(islice(rows, 256)), [])
    for part in count():
        first = next(blocks, None)
        if first is None and part:
            break
        with pq.ParquetWriter(
            directory / f"part-{part}.parquet", MERGED, compression="snappy"
        ) as writer:
            if first is None:
                break
            for block in chain([first], islice(blocks, 99)):
                writer.write_table(pa.Table.from_pylist(block, schema=MERGED))
