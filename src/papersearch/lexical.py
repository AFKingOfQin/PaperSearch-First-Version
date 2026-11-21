# AI-generated code (Codex): bounded BM25 queries against immutable published FTS5 files.
import re
import sqlite3
from contextlib import closing
from threading import Lock

from .snapshot_format import file_sha


class LexicalReader:
    def __init__(self, root):
        self.root = root
        self.verified = None
        self.lock = Lock()

    def path(self, manifest):
        entry = manifest["files"]["lexical.sqlite"]
        path = self.root / "snapshots" / manifest["snapshot_id"] / "lexical.sqlite"
        key = (path, entry["sha256"])
        with self.lock:
            if key != self.verified:
                if (
                    path.stat().st_size != entry["bytes"]
                    or file_sha(path) != entry["sha256"]
                ):
                    raise OSError("Published lexical index checksum mismatch")
                self.verified = key
        return path

    def search(self, manifest, query, field, offset, limit):
        words = list(dict.fromkeys(re.findall(r"[^\W_]+", query, re.UNICODE)))
        if not words or len(words) > 32:
            raise ValueError("Enter between 1 and 32 search terms")
        expression = " OR ".join(f'"{term}"' for term in words)
        if field in ("title", "abstract"):
            expression = f"{field} : ({expression})"
        uri = self.path(manifest).resolve().as_uri() + "?mode=ro&immutable=1"
        with closing(sqlite3.connect(uri, uri=True)) as db:
            db.row_factory = sqlite3.Row
            rows = db.execute(
                """SELECT paper_id,event_id,title,bm25(documents,3,1) AS score FROM documents
                   WHERE documents MATCH ? ORDER BY score,rowid LIMIT ? OFFSET ?""",
                (expression, limit + 1, offset),
            ).fetchall()
        return [
            dict(row, rank=i) for i, row in enumerate(rows[:limit], offset + 1)
        ], len(rows) > limit
