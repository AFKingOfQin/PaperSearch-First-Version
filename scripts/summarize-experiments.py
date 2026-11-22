# AI-generated analysis (Codex): reconcile measured outputs and retain raw observations for the report.
import argparse
import csv
import hashlib
import json
from collections import Counter
from pathlib import Path

from papersearch.contracts import digest
from papersearch.snapshot_format import file_sha

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "docs/evidence"


def read(path):
    return json.loads(path.read_bytes())


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def ranking(paths):
    trials = []
    for path in paths:
        data = read(path)
        metrics = {}
        for name, pool in data["heldout"].items():
            scores = pool["metrics"][data["frozen"]]
            metrics[name] = {
                "passage_encode_seconds": pool["document_encode_seconds"],
                "index_build": pool.get("index_build"),
                "source_sha256": pool["sha256"],
                "rows": pool["size"],
                "included": pool["included"],
                "methods": {
                    method: {k: v for k, v in m.items() if k != "ordered_ids"}
                    | {"order_sha256": digest(m["ordered_ids"])}
                    for method, m in scores.items()
                },
            }
        trials.append(
            dict(
                file_sha256=file_sha(path),
                frozen=data["frozen"],
                model=data["model"],
                metrics=metrics,
            )
        )
    for trial in trials[1:]:
        assert (trial["model"], trial["frozen"]) == (
            trials[0]["model"],
            trials[0]["frozen"],
        )
        for name, pool in trial["metrics"].items():
            first = trials[0]["metrics"][name]
            assert (pool["rows"], pool["included"], pool["source_sha256"]) == (
                first["rows"],
                first["included"],
                first["source_sha256"],
            )
            for method, values in pool["methods"].items():
                assert {k: v for k, v in values.items() if k != "query_ms"} == {
                    k: v for k, v in first["methods"][method].items() if k != "query_ms"
                }
    return trials


def check_trials(results):
    assert len(results) == 27 and len({(r["trial"], r["mode"]) for r in results}) == 27
    pipes = [r for r in results if "vector_digest" in r]
    assert (
        len(pipes) == 12
        and len({(r["vector_digest"], r["current_digest"]) for r in pipes}) == 1
    )
    streams = [r for r in results if "workers" in r]
    assert len(streams) == 15 and len({r["current_digest"] for r in streams}) == 1
    for r in streams:
        assert sum(w["commits"] for w in r["workers"]) == 384
        assert r["state"]["processed_events"] == 384 and all(
            p["lag"] == 0 for p in r["offsets"].values()
        )
        assert r["outcomes"].get("duplicate", 0) == int(r["mode"] == "after")
        if r["mode"].startswith("workers"):
            assert sum(w["commits"] > 0 for w in r["workers"]) == int(r["mode"][-1])


def system(run, repeat_paths):
    assert read(run / "completed.json")["trials"] == 3
    results = sorted(
        (read(p) for p in run.glob("*/result.json")),
        key=lambda r: (r["mode"], r["trial"]),
    )
    check_trials(results)
    spark = EVIDENCE / "phase7-batch.json"
    recheck = ROOT / "artifacts/m8-m7-recheck.json"
    assert read(spark) == read(recheck)
    save(
        EVIDENCE / "phase8-system.json",
        dict(
            environment=read(run / "environment.json"),
            host=read(ROOT / "artifacts/m8-environment.json")
            if (ROOT / "artifacts/m8-environment.json").exists()
            else None,
            verification_scripts={
                name: file_sha(ROOT / "scripts" / name)
                for name in (
                    "evaluate-rankers.py",
                    "reconcile-review.py",
                    "check-spark-results.py",
                )
            },
            completed=read(run / "completed.json"),
            inputs=read(run / "inputs.json"),
            trials=results,
            ranking_reference_sha256=file_sha(EVIDENCE / "phase6-evaluation.json"),
            ranking=ranking([EVIDENCE / "phase6-evaluation.json", *repeat_paths])[1:],
            spark_recheck_sha256=file_sha(recheck),
        ),
    )
    with (EVIDENCE / "phase8-timings.csv").open("w", newline="") as out:
        writer = csv.writer(out)
        writer.writerow(
            ["mode", "trial", "elapsed_seconds", "derived_seconds", "events_per_second"]
        )
        for r in results:
            writer.writerow(
                [
                    r["mode"],
                    r["trial"],
                    r["elapsed_seconds"],
                    r.get("derive_seconds", ""),
                    r.get("rows_per_second", ""),
                ]
            )


def capacity(folder):
    source = read(folder / "input/source.json")
    active = folder / "capacity"
    gates = [read(active / (name + ".json")) for name in ("million", "full")]
    resources = [
        json.loads(line)
        for line in (folder / "resources.jsonl").read_bytes().splitlines()
    ]
    volumes = [
        json.loads(line)
        for line in (folder / "volume-samples.jsonl").read_bytes().splitlines()
    ]
    blocks = [read(p) for p in sorted((active / "logs").glob("block-*.json"))]
    assert len(blocks) == len(source["parts"]) == gates[1]["blocks"]
    with (EVIDENCE / "phase8-blocks.csv").open("w", newline="") as out:
        writer = csv.writer(out)
        writer.writerow(
            [
                "block",
                "source_sha256",
                "source_bytes",
                "through_line",
                "start_epoch",
                "broker_ack_epoch",
                "durable_epoch",
                "applied",
                "rejected",
                "duplicates",
                "stale",
                "conflict",
                "importer_peak_bytes",
                "artifact_free_bytes",
            ]
        )
        counts = Counter()
        for b, part in zip(blocks, source["parts"], strict=True):
            assert b["through"] == part["through"]
            outcomes = (
                Counter(b["net_outcomes"])
                if b.get("net_outcomes") is not None
                else Counter(b["outcomes"]) + Counter(b["resumed_outcomes"])
            )
            counts.update(outcomes)
            writer.writerow(
                [
                    b["block"],
                    part["sha256"],
                    part["bytes"],
                    b["through"],
                    b["start"],
                    b["ack"],
                    b["durable"],
                    outcomes["applied"],
                    outcomes["rejected"],
                    outcomes["duplicate"],
                    outcomes["stale"],
                    outcomes["conflict"],
                    b["cgroup_peak"],
                    b["artifact_free"],
                ]
            )
            for gate in gates:
                if gate["blocks"] == b["block"]:
                    expected = source["gates"][gate["gate"]]
                    assert gate["expected"] == expected
                    assert (
                        gate["tables"]["rejected_events"]
                        == sum(expected["rejected"].values())
                        == counts["rejected"]
                    )
                    assert set(counts) <= {
                        "applied",
                        "stale",
                        "conflict",
                        "duplicate",
                        "rejected",
                    }
                    assert (
                        sum(counts[k] for k in ("applied", "stale", "conflict"))
                        == expected["distinct_valid_events"]
                        == gate["tables"]["processed_events"]
                    )
                    assert (
                        gate["current"] == expected["current"]
                        and gate["identity_sha256"] == expected["identity_sha256"]
                    )
                    assert (
                        sum(e["end"] for e in gate["offsets"].values())
                        == expected["lines"]
                    )
                    assert sum(counts.values()) == expected["lines"] and all(
                        e["lag"] == 0 for e in gate["offsets"].values()
                    )
                    gate["delivery_outcomes"] = dict(counts)
                    gate["active_block_seconds"] = sum(
                        x["durable"] - x["start"] for x in blocks[: b["block"]]
                    )
    save(
        EVIDENCE / "phase8-capacity.json",
        dict(
            source={k: v for k, v in source.items() if k not in ("parts", "gates")},
            input_manifest_sha256=file_sha(folder / "input/source.json"),
            gates=gates,
            attempts=[
                json.loads(r)
                for r in (folder / "capacity-attempts.jsonl").read_bytes().splitlines()
            ],
            resources=resources,
            volume_samples=volumes,
            final_volumes=read(folder / "capacity-final-volumes.json")
            if (folder / "capacity-final-volumes.json").exists()
            else None,
            tuning=read(folder / "capacity-tuning.json")
            if (folder / "capacity-tuning.json").exists()
            else None,
            batch_path_restart=read(folder / "capacity-span-restart.json")
            if (folder / "capacity-span-restart.json").exists()
            else None,
            state=read(active / "state.json"),
            recovered_blocks=[b for b in blocks if b.get("net_outcomes") is not None],
        ),
    )


def verify():
    lab = read(EVIDENCE / "phase8-system.json")
    check_trials(lab["trials"])
    assert len(lab["ranking"]) == 3
    report = read(EVIDENCE / "phase8-capacity.json")
    parts = []
    counts = Counter()
    gates = {gate["blocks"]: gate for gate in report["gates"]}
    with (EVIDENCE / "phase8-blocks.csv").open(newline="") as f:
        for number, row in enumerate(csv.DictReader(f), 1):
            assert int(row["block"]) == number
            parts.append(
                dict(
                    name=f"{number:05d}.jsonl",
                    sha256=row["source_sha256"],
                    bytes=int(row["source_bytes"]),
                    through=int(row["through_line"]),
                )
            )
            counts.update(
                {
                    key: int(row[key])
                    for key in ("applied", "rejected", "stale", "conflict")
                }
            )
            counts["duplicate"] += int(row["duplicates"])
            if number in gates:
                gate = gates[number]
                assert counts == Counter(gate["delivery_outcomes"])
                assert (
                    sum(counts.values())
                    == gate["expected"]["lines"]
                    == int(row["through_line"])
                )
                assert gate["current"] == gate["expected"]["current"]
                assert gate["identity_sha256"] == gate["expected"]["identity_sha256"]
                assert (
                    gate["decision"]["reviewed_event_id"] == report["state"]["reviewed"]
                )
    source = {
        **report["source"],
        "parts": parts,
        "gates": {g["gate"]: g["expected"] for g in gates.values()},
    }
    assert (
        len(parts) == max(gates)
        and sum(part["bytes"] for part in parts) == source["uncompressed_bytes"]
    )
    assert (
        hashlib.sha256(json.dumps(source, sort_keys=True).encode()).hexdigest()
        == report["input_manifest_sha256"]
    )
    print(
        f"PASS 27 controlled trials, {len(parts)} source blocks and both capacity gates"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="mode", required=True)
    lab = commands.add_parser("system")
    lab.add_argument("run", type=Path)
    lab.add_argument("repeats", type=Path, nargs=3)
    scale = commands.add_parser("capacity")
    scale.add_argument("folder", type=Path)
    commands.add_parser("verify")
    args = parser.parse_args()
    if args.mode == "system":
        system(args.run, args.repeats)
    elif args.mode == "capacity":
        capacity(args.folder)
    else:
        verify()
