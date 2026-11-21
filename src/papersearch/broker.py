# AI-generated code (Codex): Kafka topic configuration and broker-side progress measurements.
import json
from contextlib import closing

from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


PARTITIONS = 3


def log(kind, **fields):
    print(json.dumps(dict(kind=kind, **fields), sort_keys=True), flush=True)


def topic_info(broker, topic, *, create=False):
    with closing(KafkaAdminClient(bootstrap_servers=broker)) as admin:
        if create:
            try:
                admin.create_topics([NewTopic(topic, PARTITIONS, 1)])
            except TopicAlreadyExistsError:
                pass
        (info,) = admin.describe_topics([topic])
        if info["error_code"] or len(info["partitions"]) != PARTITIONS:
            raise ValueError(f"Expected an existing three-partition topic: {topic}")
        return admin.describe_cluster()["cluster_id"]


def offsets(reader, topic):
    partitions = [TopicPartition(topic, n) for n in range(PARTITIONS)]
    committed_offsets = {part: reader.committed(part) for part in partitions}
    low, high = reader.beginning_offsets(partitions), reader.end_offsets(partitions)
    state = {}
    for part in partitions:
        committed = committed_offsets[part]
        next_offset = committed or 0
        if not low[part] <= next_offset <= high[part]:
            raise ValueError(
                f"Missing retained range for {part}: next={next_offset}, "
                f"available=[{low[part]}, {high[part]}]. Rebuild from the archive."
            )
        state[part.partition] = dict(
            committed=committed,
            beginning=low[part],
            end=high[part],
            lag=high[part] - next_offset,
        )
    return state


def group_reader(broker, group):
    return KafkaConsumer(
        bootstrap_servers=broker,
        group_id=group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        allow_auto_create_topics=False,
        max_poll_records=16,
        session_timeout_ms=10000,
        heartbeat_interval_ms=2500,
    )
