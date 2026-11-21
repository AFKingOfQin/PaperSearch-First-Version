# AI-generated code (Codex): commands for materializing fixture work and publishing a pinned corpus.
import json
from pathlib import Path

from .derivation import (
    configure,
    configure_recipe,
    retry_job,
    run_workers,
    run_semantic,
)
from .encoder import KIND, RECIPE


def add_commands(commands):
    parser = commands.add_parser(
        "snapshot", help="Build and verify immutable search packages"
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path("/artifacts/derived"),
        help="Shared durable cache and snapshot directory",
    )
    actions = parser.add_subparsers(dest="action", required=True)
    setup = actions.add_parser(
        "configure-fixture",
        help="Select the deterministic test encoder and queue eligible papers",
    )
    setup.add_argument("--model-id", default="fixture-digest")
    setup.add_argument("--model-revision", default="v1")
    setup.add_argument("--preprocessing", default="text-v1")
    actions.add_parser(
        "configure-semantic",
        help="Queue current revisions in the declared semantic subset",
    )
    semantic = actions.add_parser(
        "embed", help="Encode leased jobs with the pinned local CPU model"
    )
    semantic.add_argument("--batch-size", type=int, default=8)
    semantic.add_argument("--max-jobs", type=int, default=100000)
    semantic.add_argument("--lease-seconds", type=int, default=180)
    actions.add_parser("retry-semantic").add_argument("job_id", type=int)
    work = actions.add_parser(
        "work", help="Run a bounded number of fixture job attempts"
    )
    work.add_argument("--workers", type=int, default=2)
    work.add_argument("--batch-size", type=int, default=16)
    work.add_argument("--max-jobs", type=int, default=1024)
    work.add_argument("--lease-seconds", type=int, default=60)
    actions.add_parser(
        "retry", help="Release a terminally failed fixture job for retry"
    ).add_argument("job_id", type=int)
    actions.add_parser(
        "build", help="Build, validate and publish one consistent package"
    )
    actions.add_parser(
        "verify", help="Verify the active package including every row mapping"
    )
    actions.add_parser(
        "status", help="Show active coverage, freshness, job counts and recent errors"
    )


def run(args, engine):
    from .snapshots import active_snapshot, publish_snapshot, status

    if args.action == "configure-fixture":
        result = configure(
            engine, args.model_id, args.model_revision, args.preprocessing
        )
    elif args.action == "configure-semantic":
        result = configure_recipe(engine, RECIPE)
    elif args.action == "embed":
        result = run_semantic(
            engine, args.root, args.batch_size, args.max_jobs, args.lease_seconds
        )
    elif args.action == "retry-semantic":
        retry_job(engine, args.job_id, KIND)
        result = dict(requeued=args.job_id)
    elif args.action == "work":
        result = run_workers(
            engine,
            args.root,
            args.workers,
            args.batch_size,
            args.max_jobs,
            args.lease_seconds,
        )
    elif args.action == "retry":
        retry_job(engine, args.job_id)
        result = dict(requeued=args.job_id)
    elif args.action == "build":
        result = publish_snapshot(engine, args.root)
    elif args.action == "verify":
        _, result = active_snapshot(engine, args.root)
    else:
        result = status(engine, args.root)
    print(json.dumps(result, sort_keys=True))
    return int(bool(result.get("errors") or result.get("lost_leases")))
