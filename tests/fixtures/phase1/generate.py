# AI-generated code (Codex): deterministic synthetic development fixture.
import argparse
import json
from pathlib import Path


def fixture_bytes() -> bytes:
    cases = Path(__file__).with_name("cases.jsonl").read_bytes()
    base = []
    for i in range(1000 - len(cases.splitlines())):
        row = dict(
            id=f"sample-{i:04d}",
            title=f"Sample {i:04d}",
            abstract="Synthetic development record.",
            source_version=1,
            source_updated_at="2020-01-01T00:00:00Z",
            categories=["fixture"],
        )
        base.append(json.dumps(row, separators=(",", ":")).encode() + b"\n")
    return b"".join(base) + cases


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Write the 1,000-record synthetic fixture to a new file"
    )
    parser.add_argument("path", type=Path)
    target = parser.parse_args().path
    target.parent.mkdir(parents=True, exist_ok=True)
    data = fixture_bytes()
    if (
        target.is_file()
        and target.stat().st_size == len(data)
        and target.read_bytes() == data
    ):
        print(f"Existing fixture matches: {target}")
    else:
        with target.open("xb") as out:
            out.write(data)
        print(f"Wrote 1,000 records to {target}")
