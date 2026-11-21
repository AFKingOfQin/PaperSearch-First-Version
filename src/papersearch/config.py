# AI-generated code (Codex): Phase 0 configuration and storage checks.
import os
import shutil
from pathlib import Path

from dotenv import dotenv_values
from sqlalchemy.engine import URL


def settings(env_file: Path) -> dict[str, str]:
    if not env_file.is_file():
        raise ValueError(f"Missing configuration file {env_file}")
    values = {k: v for k, v in dotenv_values(env_file, interpolate=False).items() if v is not None}
    return {**values, **os.environ}


def reserve_gib(config: dict[str, str], scale: bool) -> int:
    key, default = ("SCALE_FREE_GIB", "100") if scale else ("CORE_FREE_GIB", "5")
    value = config.get(key, default)
    if not value.isascii() or not value.isdecimal() or int(value) < 1:
        raise ValueError(f"{key} must be a positive integer")
    return int(value)


def disk_preflight(config: dict[str, str], scale: bool) -> bool:
    minimum = reserve_gib(config, scale)
    docker_dir = config.get("DOCKER_DISK_DIR")
    if not docker_dir or not Path(docker_dir).is_dir():
        raise ValueError("Set DOCKER_DISK_DIR to Docker Desktop's existing disk image directory")
    artifact = Path(config.get("ARTIFACT_DIR") or "./artifacts").resolve()
    if artifact.exists() and not artifact.is_dir():
        raise ValueError(f"ARTIFACT_DIR is a file: {artifact}")
    # The artifact directory can be new. Inspect its nearest existing parent.
    while not artifact.exists() and artifact != artifact.parent:
        artifact = artifact.parent
    if not artifact.is_dir():
        raise ValueError("ARTIFACT_DIR must have an accessible parent directory")
    passed = True
    for label, path in [("Artifacts", artifact), ("Docker data disk", Path(docker_dir).resolve())]:
        free = shutil.disk_usage(path).free
        ok = free >= minimum * 2**30
        print(f"{label} {path}: {free / 2**30:.2f} GiB free, require {minimum} GiB: {'PASS' if ok else 'BLOCKED'}")
        passed = passed and ok
    return passed


def mysql_url(config: dict[str, str]) -> URL:
    password = config.get("MYSQL_PASSWORD")
    if not password:
        raise ValueError("Set MYSQL_PASSWORD in the local environment or .env")
    return URL.create("mysql+mysqlconnector", username="papersearch", password=password,
                      host=config.get("MYSQL_HOST", "mysql"), database="papersearch")
