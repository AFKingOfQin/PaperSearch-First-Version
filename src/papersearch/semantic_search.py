# AI-generated code (Codex): exact CPU cosine ranking over the ID-aligned published subset.
import sqlite3
from contextlib import closing
from threading import Lock

import numpy as np
import pyarrow.parquet as pq

from .encoder import DIM, RECIPE, Encoder
from .snapshot_format import file_sha


def cosine_order(matrix, query, row_ids):
    scores = np.einsum("ij,j->i", matrix, query)
    return np.lexsort((row_ids, -scores)), scores


class SemanticReader:
    def __init__(self, lexical):
        self.lexical = lexical
        self.lock = Lock()
        self.encoder = None
        self.identity = None

    def search(self, manifest, query, offset, limit):
        if not query.strip():
            raise ValueError("Enter a search query")
        if not manifest["coverage"]["semantic"] or manifest["recipe"] != RECIPE:
            raise OSError("This snapshot has no supported semantic index")
        lexical_path = self.lexical.path(manifest)
        with self.lock:
            # Readers hold the lock through scoring. Drop the old arrays before loading a replacement.
            if self.encoder is None:
                try:
                    self.encoder = Encoder()
                except (ImportError, OSError, RuntimeError, ValueError) as exc:
                    raise OSError("Pinned CPU model unavailable") from exc
            if self.identity != manifest["snapshot_id"]:
                self.identity = self.matrix = self.row_ids = None
                path = lexical_path.with_name("semantic.parquet")
                if file_sha(path) != manifest["files"]["semantic.parquet"]["sha256"]:
                    raise OSError("Semantic index checksum mismatch")
                n = manifest["coverage"]["semantic"]
                matrix = np.empty((n, DIM), dtype="<f4")
                ids = np.empty(n, dtype=np.int64)
                start = 0
                with pq.ParquetFile(path) as file:
                    for batch in file.iter_batches(
                        batch_size=256, columns=["row_id", "value"], use_threads=False
                    ):
                        end = start + len(batch)
                        ids[start:end] = batch.column(0).to_numpy()
                        matrix[start:end] = np.frombuffer(
                            b"".join(batch.column(1).to_pylist()), dtype="<f4"
                        ).reshape(-1, DIM)
                        start = end
                if start != n:
                    raise OSError("Semantic index coverage mismatch")
                self.matrix, self.row_ids, self.identity = (
                    matrix,
                    ids,
                    manifest["snapshot_id"],
                )
            vector, lengths = self.encoder.encode([query], query=True)
            order, scores = cosine_order(self.matrix, vector[0], self.row_ids)
            page = [
                (int(self.row_ids[i]), float(scores[i]))
                for i in order[offset : offset + limit]
            ]
        uri = lexical_path.resolve().as_uri() + "?mode=ro&immutable=1"
        with closing(sqlite3.connect(uri, uri=True)) as db:
            db.row_factory = sqlite3.Row
            hits = [
                dict(
                    db.execute(
                        "SELECT paper_id,event_id,title FROM documents WHERE rowid=?",
                        (number,),
                    ).fetchone(),
                    rank=rank,
                    score=score,
                )
                for rank, (number, score) in enumerate(page, offset + 1)
            ]
        return hits, offset + limit < len(order), lengths[0]
