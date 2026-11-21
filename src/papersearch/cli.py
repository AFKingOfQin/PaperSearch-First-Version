# AI-generated code (Codex): configuration checks and independent service probes.
import argparse
import os
import sys
from pathlib import Path

from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaError
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from .config import disk_preflight, mysql_url, reserve_gib, settings
from .doctor import doctor


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
    args = parser.parse_args()
    try:
        if args.command == "doctor":
            return 0 if doctor() else 1
        file = args.env_file or Path(".env")
        config = settings(file) if args.env_file or file.exists() else dict(os.environ)
        if args.command == "config":
            print(f"Core reserve {reserve_gib(config, False)} GiB, scale reserve {reserve_gib(config, True)} GiB")
        elif args.command == "preflight":
            return 0 if disk_preflight(config, args.scale) else 1
        else:
            smoke(args.service, config)
    except (ValueError, OSError, KafkaError, SQLAlchemyError) as error:
        print(f"BLOCKED: {error}", file=sys.stderr)
        return 1
    return 0
