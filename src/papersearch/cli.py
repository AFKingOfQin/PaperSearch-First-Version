# AI-generated code (Codex): configuration checks and independent service probes.
import argparse
import json
import os
import sys
from pathlib import Path

from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaError
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from .config import disk_preflight, mysql_url, reserve_gib, settings
from .doctor import doctor
from .reference import process_files
from .storage import commands as storage
from . import snapshot_cli, stream_cli


def smoke(service: str, config: dict[str, str]) -> None:
    if service == "mysql":
        engine = create_engine(mysql_url(config), connect_args={"connection_timeout": 5, "read_timeout": 5})
        with engine.connect() as conn:
            one, version, server_uuid = conn.execute(text("SELECT 1, VERSION(), @@server_uuid")).one()
        if one != 1:
            raise ValueError("Unexpected SELECT 1 response")
        print(f"PASS MySQL SELECT 1 = {one}, version {version}, server UUID {server_uuid}")
    else:
        client = KafkaAdminClient(bootstrap_servers=config.get("KAFKA_BOOTSTRAP", "kafka:9092"),
                                  request_timeout_ms=6000, api_version_auto_timeout_ms=5000)
        try:
            cluster = client.describe_cluster()
            if not cluster["brokers"] or not cluster["cluster_id"]:
                raise ValueError("Kafka returned incomplete cluster metadata")
            print(f"PASS Kafka cluster {cluster['cluster_id']}, brokers {len(cluster['brokers'])}")
        finally:
            client.close()


def main() -> int:
    parser = argparse.ArgumentParser(prog="papersearch")
    parser.add_argument("--env-file", type=Path, help="Local configuration file (default: .env when present)")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("doctor", help="Inspect host tools and the Linux engine")
    commands.add_parser("config", help="Check configuration without displaying credentials")
    preflight = commands.add_parser("preflight", help="Check host storage before starting a workload")
    preflight.add_argument("--scale", action="store_true", help="Apply the larger scale-run reserve")
    commands.add_parser("smoke", help="Connect to one real service").add_argument("service", choices=("mysql", "kafka"))
    reference = commands.add_parser("reference", help="Compute canonical paper revisions from local JSONL")
    reference.add_argument("paths", type=Path, nargs="+")
    reference.add_argument("--format", choices=("fixture", "arxiv", "event"), required=True)
    reference.add_argument("--collected-at", help="Archive collection timestamp with UTC offset (raw formats)")
    reference.add_argument("--batch-id", help="Archive batch identity (raw formats)")
    reference.add_argument("--summary", action="store_true", help="Omit the full canonical paper mapping")
    storage.add_commands(commands)
    stream_cli.add_commands(commands)
    snapshot_cli.add_commands(commands)
    args = parser.parse_args()
    try:
        if args.command == "reference":
            if args.format != "event" and not (args.batch_id and args.collected_at):
                reference.error("raw records require --batch-id and --collected-at")
            result = process_files(args.paths, args.format, args.batch_id, args.collected_at)
            if args.summary:
                del result["papers"]
            print(json.dumps(result, sort_keys=True, indent=2))
            return 0
        if args.command == "doctor":
            return 0 if doctor() else 1
        file = args.env_file or Path(".env")
        config = settings(file) if args.env_file or file.exists() else dict(os.environ)
        if args.command == "config":
            print(f"Core reserve {reserve_gib(config, False)} GiB, scale reserve {reserve_gib(config, True)} GiB")
        elif args.command == "preflight":
            return 0 if disk_preflight(config, args.scale) else 1
        elif args.command == "db":
            storage.run(args, create_engine(mysql_url(config)))
        elif args.command == "stream":
            stream_cli.run(args, config)
        elif args.command == "snapshot":
            return snapshot_cli.run(args, create_engine(mysql_url(config)))
        else:
            smoke(args.service, config)
    except (ValueError, OSError, KafkaError, SQLAlchemyError) as error:
        print(f"BLOCKED: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
