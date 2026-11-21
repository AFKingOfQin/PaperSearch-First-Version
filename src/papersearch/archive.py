# AI-generated code (Codex): immutable JSONL batch archives and crash-safe file publication.
import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path

from .adapters import raw_lines
from .contracts import digest, provenance, utc


def sync_dir(path: Path):
    if os.name == "posix":
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def write_atomic(path: Path, data: bytes):
    out = tempfile.NamedTemporaryFile(dir=path.parent, delete=False)
    tmp = Path(out.name)
    try:
        with out:
            out.write(data)
            out.flush()
            os.fsync(out.fileno())
        os.replace(tmp, path)
        sync_dir(path.parent)
    finally:
        tmp.unlink(missing_ok=True)


def inspect_batch(directory: Path) -> dict:
    manifest = json.loads((directory / "manifest.json").read_bytes())
    if digest(manifest) != directory.name or manifest["version"] != 1:
        raise ValueError("Batch manifest identity mismatch")
    with (directory / "records.jsonl").open("rb") as stream:
        if hashlib.file_digest(stream, "sha256").hexdigest() != manifest["sha256"]:
            raise ValueError("Archived JSONL checksum mismatch")
    return manifest


def archive(path: Path, root: Path, fmt: str, batch: str, collected: str) -> Path:
    batch, collected = provenance(batch, 256), utc(collected)
    root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=root) as staging:
        directory = Path(staging) / "batch"
        directory.mkdir()
        raw = directory / "records.jsonl"
        with path.open("rb") as source, raw.open("xb") as out:
            shutil.copyfileobj(source, out, length=1024 * 1024)
            out.flush()
            os.fsync(out.fileno())
        with raw.open("rb") as stream:
            checksum = hashlib.file_digest(stream, "sha256").hexdigest()
        manifest = dict(
            version=1,
            format=fmt,
            batch_id=batch,
            collected_at=collected,
            sha256=checksum,
            records=sum(1 for _ in raw_lines(raw)),
            bytes=raw.stat().st_size,
        )
        target = root / digest(manifest)
        write_atomic(
            directory / "manifest.json", json.dumps(manifest, sort_keys=True).encode()
        )
        try:
            directory.rename(target)
        except OSError:
            if not target.is_dir() or inspect_batch(target) != manifest:
                raise
        sync_dir(root)
    return target
