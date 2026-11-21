# AI-generated code (Codex): CLI access to the MySQL corpus and screening decisions.
import json
from pathlib import Path

from .metadata import apply_file, export_rows
from .reviews import create_project, read_decision, save_decision


def add_commands(commands):
    db = commands.add_parser("db", help="Import, export, and screen papers in MySQL")
    actions = db.add_subparsers(dest="action", required=True)
    ingest = actions.add_parser(
        "apply", help="Apply one local JSONL file transactionally"
    )
    ingest.add_argument("path", type=Path)
    ingest.add_argument(
        "--format", required=True, choices=("fixture", "arxiv", "event")
    )
    ingest.add_argument("--batch-id")
    ingest.add_argument("--collected-at")
    actions.add_parser(
        "export", help="Stream one generation as a JSONL header and paper records"
    )
    project = actions.add_parser(
        "project", help="Create a review project and print its ID"
    )
    project.add_argument("topic")
    project.add_argument("--criteria", default="")
    decide = actions.add_parser(
        "decide", help="Save a label for the revision you reviewed"
    )
    decide.add_argument("project")
    decide.add_argument("paper")
    decide.add_argument("reviewed_event")
    decide.add_argument("label", choices=("relevant", "irrelevant", "uncertain"))
    decide.add_argument("--note", default="")
    decide.add_argument(
        "--request-id", required=True, help="UUID, reused when retrying the same save"
    )
    read = actions.add_parser(
        "decision", help="Read the current decision and reconsideration flag"
    )
    read.add_argument("project")
    read.add_argument("paper")


def run(args, engine):
    if args.action == "apply":
        result = apply_file(
            engine, args.path, args.format, args.batch_id, args.collected_at
        )
    elif args.action == "export":
        for row in export_rows(engine):
            print(json.dumps(row))
        return
    elif args.action == "project":
        result = create_project(engine, args.topic, args.criteria)
    elif args.action == "decide":
        result = save_decision(
            engine,
            args.project,
            args.paper,
            args.reviewed_event,
            args.label,
            args.note,
            args.request_id,
        )
    else:
        result = read_decision(engine, args.project, args.paper)
    print(json.dumps(result, default=str))
