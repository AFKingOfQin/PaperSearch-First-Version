# AI-generated code (Codex): stream command-line entry points.
from contextlib import closing
from pathlib import Path

from sqlalchemy import create_engine

from .archive import archive
from .config import mysql_url
from .broker import group_reader, log, offsets, topic_info
from .ingestion import consume
from .publication import publish


def add_commands(commands):
    stream = commands.add_parser(
        "stream", help="Archive, publish and consume paper revisions"
    )
    actions = stream.add_subparsers(dest="action", required=True)
    save = actions.add_parser("archive")
    save.add_argument("path", type=Path)
    save.add_argument("--archive-root", type=Path, default=Path("/artifacts/archives"))
    save.add_argument("--batch-id", required=True)
    save.add_argument("--collected-at", required=True)
    save.add_argument("--format", required=True, choices=("fixture", "arxiv", "event"))
    actions.add_parser("init").add_argument("topic")
    send = actions.add_parser("publish")
    send.add_argument("topic")
    send.add_argument("archive", type=Path)
    send.add_argument("--replay", action="store_true")
    send.add_argument(
        "--group",
        default="papersearch-ingest",
        help="Consumer group to monitor for backpressure",
    )
    send.add_argument(
        "--max-lag",
        type=int,
        default=4096,
        help="Pause before exceeding this outstanding record count",
    )
    for name in ("consume", "status"):
        action = actions.add_parser(name)
        action.add_argument("topic")
        action.add_argument("--group", default="papersearch-ingest")
        if name == "consume":
            action.add_argument(
                "--drain",
                action="store_true",
                help="Exit when committed offsets reach the topic ends",
            )
            action.add_argument(
                "--quarantine", type=Path, default=Path("/artifacts/quarantine")
            )
            action.add_argument(
                "--test-fault", choices=("before-commit", "after-commit")
            )
            action.add_argument("--test-at")


def run(args, config):
    broker = config.get("KAFKA_BOOTSTRAP", "kafka:9092")
    if args.action == "archive":
        print(
            archive(
                args.path,
                args.archive_root,
                args.format,
                args.batch_id,
                args.collected_at,
            )
        )
    elif args.action == "init":
        log(
            "topic_ready",
            topic=args.topic,
            partitions=3,
            cluster=topic_info(broker, args.topic, create=True),
        )
    elif args.action == "publish":
        publish(
            broker,
            args.topic,
            args.archive,
            args.replay,
            group=args.group,
            max_lag=args.max_lag,
        )
    elif args.action == "status":
        topic_info(broker, args.topic)
        with closing(group_reader(broker, args.group)) as reader:
            log("offsets", group=args.group, partitions=offsets(reader, args.topic))
    else:
        engine = create_engine(
            mysql_url(config),
            pool_pre_ping=True,
            connect_args={
                "connection_timeout": 3,
                "read_timeout": 10,
                "write_timeout": 10,
            },
        )
        consume(
            engine,
            broker,
            args.topic,
            args.group,
            args.quarantine,
            drain=args.drain,
            fault=args.test_fault,
            at=args.test_at,
        )
