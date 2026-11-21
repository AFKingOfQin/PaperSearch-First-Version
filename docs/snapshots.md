# Building and reading a search snapshot

MySQL makes a paper durable when its revision transaction commits. A lexical reader sees the revision when a validated package containing it becomes active. Kafka lag, pending jobs, and snapshot freshness measure different stages of that process.

## Run on the local fixture

Use the Windows setup in the README. Import the fixture using the [Kafka ingestion commands](ingestion.md). Then run the following from the same checkout and Compose project:

```powershell
.\.venv\Scripts\papersearch preflight
docker compose --profile core up -d --wait mysql
docker compose build snapshot
docker compose run --rm --no-deps --entrypoint alembic snapshot upgrade head
docker compose run --rm --no-deps snapshot snapshot configure-fixture
docker compose run --rm --no-deps snapshot snapshot work --workers 2 --batch-size 32 --max-jobs 2048
docker compose run --rm --no-deps snapshot snapshot build
docker compose run --rm --no-deps snapshot snapshot status
docker compose run --rm --no-deps snapshot snapshot verify
docker compose --profile core stop mysql
```

In these commands the first `snapshot` selects the Compose service, and the second selects the PaperSearch CLI command. The service uses the shared artifact mount, one CPU and a 768 MiB memory limit by default. Install the pinned requirements in the host virtual environment to run its Parquet tests. [PyArrow 23.0.1 release](https://pypi.org/project/pyarrow/23.0.1/)

`configure-fixture` selects a deterministic test encoder for `fixture:` paper IDs and queues eligible current records in pages of 128. Subsequent imports enqueue a winning revision under the selected recipe in the metadata transaction. Re-running configuration also resumes interrupted backfill. The upgrade preserves the original BGE job identity until you explicitly select the fixture recipe. Previously queued BGE jobs remain visible under `embedding`. The fixture worker claims only `test-digest-v1` jobs.

## Cache and lease recovery

The cache key hashes the normalized title/abstract hash, artifact kind, model ID, model revision and preprocessing identity. Paper ID belongs to the **job** key. Two papers with identical text can therefore reuse one cache entry. The test encoder generates 32 deterministic bytes per key. Changing `--model-id`, `--model-revision` or `--preprocessing` on `configure-fixture` exercises identity invalidation without downloading a model.

A worker claims a bounded batch with MySQL leases, packs distinct misses into one shard, verifies its contents, and records the shard checksum and slot for each key. Job completion and its cache entry commit together only while the worker holds the unexpired lease. A crash after writing a shard can leave reusable bytes on disk. A later worker can reclaim the job and validate the existing shard or generate a new batch. An old-text job may complete after an abstract changes. Snapshot selection joins the current revision's text hash and the full recipe before including any bytes.

`work` defaults to two workers, 16 jobs per batch, 1,024 attempts per invocation and a 60-second lease. Its JSON includes claimed/completed counts, computations, reuse and errors when applicable. A repeated delivery can yield zero claims because the original job already completed. Distinct jobs sharing a cache entry produce hits. Concurrent workers may compute the same miss, while MySQL admits one cache entry per identity. Bounds are 1–4 workers and 1–64 jobs per batch. Run the command again to continue a longer queue.

On an artifact I/O error, inspect `snapshot status` for the job ID, attempts and reason, then correct the underlying storage problem. A pending job can be claimed on the next run. Three unsuccessful attempts make it terminal. To recover a terminal fixture job, run `snapshot retry JOB_ID` followed by `snapshot work`. Keep the same mounted store for the database, including its hidden `.stores` directory. A different path to the same mount can use its existing marker. A writer pointed at an unrelated directory fails before claiming work or switching the active package.

## What the reader opens

The builder reads the generation, selected recipe, current revisions and cache locations in one InnoDB repeatable-read transaction. It writes bounded pages into a temporary directory. One immutable package contains:

| File | Contents |
|---|---|
| `corpus.parquet` | Canonical paper and event IDs, revision precedence, hashes, title, abstract, author/category lists, publication date, source URL and DOI. Rows are ordered by paper ID and have a one-based row ID. |
| `lexical.sqlite` | A SQLite FTS5 term index over title and abstract. Its row ID, paper ID, event ID and text hash identify the same Parquet row. |
| `test-vectors.parquet` | Eligible completed test artifacts with a dense zero-based vector row, corpus row ID, paper ID, event ID, text hash, cache key and 32-byte value. Pending or stale values are omitted. |
| `manifest.json` | Snapshot ID, UTC creation time, generation, artifact epoch, recipe and eligibility, coverage, canonical corpus hash, byte sizes and SHA-256 checksums. |

The validator reopens the files and checks the schemas, canonical digest, lexical rows and every vector mapping. Only after validation and a same-filesystem rename does a short SQL transaction record the manifest checksum and switch the active pointer. A concurrent publication or recipe change invalidates a losing builder's pointer update. Metadata can advance during a build. The package retains the generation it actually read.

Readers start from the database pointer and pin that immutable snapshot ID. Directory listings can contain partial work or a renamed package whose publisher died before the pointer switch. Old packages remain readable by their pinned IDs. Generated files, directories from interrupted builds, and dedicated verification volumes remain in local storage.

`status` shows corpus and active generations, coverage, job states, recent errors, and whether cached artifacts or the selected recipe changed since publication. `verify` checks the complete active package. The lexical index supports term lookup. Ranking, the reviewer UI, and semantic model inference belong to later milestones.
