# Archive, publish and consume a batch

Run the following in PowerShell from the repository root with the Windows setup from the README. The commands use the default artifact directory, the default Compose project and a private single-broker Kafka topic. If you configured another host artifact directory, generate the fixture there. First use requires the container images and build dependencies. The data and verification inputs are local.

```powershell
.\.venv\Scripts\papersearch preflight
.\.venv\Scripts\python tests/fixtures/phase1/generate.py artifacts/phase1/development.jsonl
docker compose --profile core up -d --wait mysql kafka
docker compose build cli
docker compose run --rm --no-deps --entrypoint alembic cli upgrade head
$topic = 'papersearch.updates'
docker compose run --rm --no-deps cli stream init $topic
$batch = docker compose run --rm --no-deps cli stream archive /artifacts/phase1/development.jsonl --format fixture --batch-id development --collected-at 2020-02-01T00:00:00Z
docker compose run --rm --no-deps cli stream publish $topic $batch
docker compose run --rm --no-deps cli stream consume $topic --drain
docker compose run --rm --no-deps cli stream status $topic
docker compose --profile core stop mysql kafka
```

On an empty database, expect 986 current papers, 993 event markers, five distinct malformed records and zero ingestion lag. Use `db export` to read the corpus. The default group is `papersearch-ingest`. Omit `--drain` to run a continuous worker. To replay the archive deliberately, start the services and run the same publication command with `--replay`. To re-read retained messages under a new group, pass `--group <new-name>` to the consumer.

## Broker acknowledgements and archive recovery

Archiving copies exact source bytes and publishes a directory containing the completed JSONL and its manifest. The directory name is the digest of the manifest. It identifies the format, batch, collection time, byte count, line count and file SHA-256. Publication checks both the manifest identity and raw checksum. Valid envelopes are keyed by stable paper ID, which keeps revisions of one paper ordered in one of three partitions. An invalid source line travels as a rejection notice keyed by its raw hash, with its reason and archived location.

The publisher sends up to 32 records at a time and waits for every broker acknowledgement before replacing the local checkpoint. A restart checks the last acknowledged record against the archive and Kafka, then resumes at the next source line. An interruption before checkpoint replacement can resend the last window. Use one publisher at a time for a batch and topic. The worker's event ledger handles repeated deliveries.

The publisher monitors the target group's committed lag across all partitions. With the default limit of 4,096 outstanding records, a window that would exceed the limit pauses publication with a nonzero exit. Run the consumer and repeat the same publish command to resume. Set `--group` on both commands if using a named group. Set `--max-lag` on the publisher to change the limit.

## SQL durability and offsets

A worker polls at most 16 messages, checks each partition's expected next offset, and processes its records in order. For each delivery, the M2 transaction either applies the revision, recognizes a duplicate, or records a durable rejection. After MySQL commits, the worker synchronously commits that partition's next offset. Its JSON log includes the partition, input offset, committed next offset, outcome and identity. The drain summary gives per-run outcome counts. The status command reads beginning, end and committed offsets from Kafka. [Kafka consumer offset semantics](https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html)

A crash before SQL commit leaves the transaction available for retry. A crash after SQL commit can redeliver the event. The unique event marker keeps that second delivery from adding another revision or job. A malformed Kafka value is saved by content hash under the artifact directory before MySQL records its rejection. SQL errors retry the same offset with bounded backoff. After four failed attempts the worker exits and can be restarted when MySQL is healthy.

Kafka retention is bounded. If the required offset lies outside the retained range, use a **new three-partition topic**, republish the required archives in source order and drain a new group into the existing database. Keep archive files, checkpoints and MySQL volumes across restarts. Zero Kafka lag means ingestion has caught up. Derived jobs have their own lifecycle.

## Repeat the failure experiment

`scripts/verify-ingestion.ps1` creates a separate Compose project and three isolated schemas. Each trial first screens the same river paper. A malformed wire value precedes a healthy revision on the same partition. Another healthy record follows the fault target. The verifier inspects both Kafka and MySQL while each worker is down. It then compares the three databases and confirms that a fresh group and an explicit batch replay preserve state.

Faults require both `PAPERSEARCH_TEST_FAULTS=1` and an explicit boundary and partition/offset. The verifier supplies them only to the disposable test worker. Its success line prints the Compose project name. The corresponding host artifact directory holds the input manifests and individual publisher, consumer, retry and offset logs. Each verifier run retains its dedicated volumes for inspection.
