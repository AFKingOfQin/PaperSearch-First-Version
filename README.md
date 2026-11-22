# PaperSearch

PaperSearch lets you search a published paper collection by title and abstract, inspect revisions and save project-specific screening decisions. Kafka delivers metadata updates to MySQL. The reviewer keeps the revision you labeled and flags changed text for reconsideration. [Reviewer setup, offline demo and real-data reproduction](docs/reviewer.md)

The reviewer offers BM25 over 100,000 real papers and exact cosine search over a published subset. It shows the number of papers each method can search and how often the encoder shortened an input. [Semantic search setup and verification](docs/semantic-ranking.md)

The laptop benchmarks measure what happens when papers arrive, text changes, a consumer crashes, or metadata outgrows the published index. [Measured results and reproduction commands](docs/results.md)

## Start here

On a new Windows checkout, follow the [setup and recovery runbook](docs/runbook.md). It takes you from an empty database to a search, saved decision, restart, revised abstract and CSV export. After setup, repeat the offline check with `.\.venv\Scripts\python scripts/review_demo.py`.

| Collection | Current metadata | Searchable with BM25 | Searchable with cosine |
|---|---:|---:|---:|
| Offline reviewer demo | 1 | 1 | 0 |
| Measured arXiv reviewer | 100,000 | 100,000 | 4,878 |
| Separate full v287 capacity database | 3,050,173 | 0 | 0 |

Read the [architecture and code walkthrough](docs/architecture.md) for the transaction and publication boundaries, the [data dictionary](docs/data_dictionary.md) for the durable records, and the [CS544 mapping](docs/cs544_mapping.md) for the experiments behind each design choice. The [final acceptance checklist](docs/ACCEPTANCE_CHECKLIST.md) links the project evidence.

## Verify the Spark batch merge

With the pinned Spark image cached, run `.\.venv\Scripts\python scripts/verify-spark-fixture.py` from the configured Windows checkout. It checks the offline batch output against the original revision fixture. The larger comparison uses the retained 100,000-paper publication, measures fresh and warmed Spark applications, and checks every merged record and storage row. [Batch setup, input contract and reproduction commands](docs/spark-batch.md)

## Verify the reviewer workflow

With Docker Desktop and the local credentials configured, prepare the images and run the demo from the repository root.

```powershell
.\.venv\Scripts\python -m pip install -r requirements-review.lock
docker compose --profile core pull mysql kafka
docker compose --profile snapshot --profile review build snapshot api
.\.venv\Scripts\python scripts/review_demo.py
```

The last command runs offline and prints `PASS offline reviewer demo` after testing a saved decision through a service restart, an abstract revision and CSV export. Add `--keep-running` to explore the fixture in a browser. [Measured reviewer results](docs/evidence/phase5-review.md)

## Verify snapshot publication

From the configured Windows checkout with Docker Desktop running, execute the local recovery demo:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verify-snapshots.ps1
```

The demo interrupts a worker and two snapshot builds, imports the 1,000-line fixture through Kafka, then reopens the published package after restarting MySQL. Look for `PASS M4 snapshot publication and recovery`. The expected package has 986 canonical papers, 986 searchable lexical rows and 986 deterministic test vectors. Real semantic coverage is zero. The script stops its isolated services and retains the run directory and volumes. [Manual commands and package format](docs/snapshots.md) · [Recovery evidence](docs/evidence/phase4-snapshots.txt) · [Sample published manifest](docs/evidence/phase4-manifest.json)

## Verify Kafka ingestion recovery

With Docker Desktop running and the Windows environment below configured, run:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verify-ingestion.ps1
```

The verifier compares three fresh MySQL databases fed by the same local Kafka topic. It kills two workers on opposite sides of the SQL commit, stops the test database during a retry, then checks recovered papers, labels, jobs and partition offsets against the uninterrupted run. Look for `PASS M3 Kafka recovery`. The verifier stops its services and retains its dedicated volumes, archived fixtures and per-command logs. [Manual import and recovery commands](docs/ingestion.md) · [Recorded verification](docs/evidence/phase3-kafka.txt)

## Verify database recovery

With Docker Desktop running and the Windows environment below configured, run this command from the repository root.

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verify-storage.ps1
```

The verifier creates a uniquely named Compose project, applies the migration, races four deliveries of one paper, saves decisions in two projects, replays the 1,000-line fixture, then restarts MySQL and reclaims an expired job. Look for `PASS M2 MySQL persistence and recovery`. Each run stops its database and retains a dedicated volume and an artifact directory under the printed project name. The images and Python packages must be cached for offline use. For a manual import and screening session, see the [storage guide](docs/storage.md).

## Run the offline reference processor

With the project virtual environment installed, run these commands from the repository root in PowerShell. They generate exactly 1,000 synthetic input lines and compute which paper revision should be current.

```powershell
.\.venv\Scripts\python tests/fixtures/phase1/generate.py artifacts/phase1/development.jsonl
.\.venv\Scripts\papersearch reference --format fixture --batch-id development --collected-at 2020-02-01T00:00:00Z --summary artifacts/phase1/development.jsonl
.\.venv\Scripts\python -m pytest -q tests/unit/test_reference.py
```

Expect **986 current papers**, 2 duplicate deliveries, 5 malformed deliveries and 2 conflicting revision groups. Omit `--summary` to inspect the records. The same command accepts several paths in delivery order, including repeated copies of a batch. It runs locally without starting services. See the [record and event contract](docs/record-contract.md), [expected results](tests/fixtures/phase1/expected.json), and [M1 verification](docs/evidence/phase1-reference.txt).

## Check the skeleton on Windows

From the repository root in PowerShell, use CPython 3.14 (tested with 3.14.4). If this checkout already has the project virtual environment, start with the second block. Initial installation needs the packages from PyPI.

```powershell
py -3.14 -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements-dev.lock
.\.venv\Scripts\python -m pip install --no-build-isolation --no-deps -e .
```

```powershell
.\.venv\Scripts\papersearch --env-file .env.example config
.\.venv\Scripts\python -m pytest -q
.\.venv\Scripts\ruff check src migrations tests
.\.venv\Scripts\python -m alembic heads
.\.venv\Scripts\papersearch doctor
```

Expect 5 GiB and 100 GiB in the configuration output and `0002_search_snapshots (head)` from Alembic. The doctor reports host RAM, disk, WSL distributions and Docker engine limits. It exits with code 1 when a prerequisite is missing. The commands above work offline once dependencies are installed.

## Enable the service runtime

On the verified PaperSearch laptop, WSL 2 and Docker Desktop are installed and the local `.env` points to Docker's data disk. Start Docker Desktop and proceed to **Verify Phase 0 with real services**. The following setup steps apply to a new installation.

1. On this laptop, look in **Task Manager → Performance → CPU → Virtualization**. Windows reports an active hypervisor and a false firmware-virtualization flag. If Task Manager confirms virtualization is disabled, enable Intel virtualization in UEFI before continuing. [Microsoft virtualization setup](https://support.microsoft.com/en-us/windows/enable-virtualization-on-windows-c5578302-6e43-4b4b-a449-8ced115f58e1)
2. In an Administrator PowerShell run `wsl --install`, restart Windows, and finish the distribution's first launch. Check `wsl --version`, `wsl --status` and `wsl -l -v`. Confirm the distribution shows version 2. Use `wsl --update` if Docker requires a newer WSL package. [Microsoft WSL installation](https://learn.microsoft.com/en-us/windows/wsl/install)
3. Install/start Docker Desktop with its WSL 2 backend and Linux containers. Run `docker compose version` and `docker info`. Record the Docker Desktop disk-image location from **Settings → Resources → Advanced**. This host path identifies the drive that must have space for Docker's named volumes. [Windows installation](https://docs.docker.com/desktop/setup/install/windows-install/) · [WSL backend and data location](https://docs.docker.com/desktop/features/wsl/)
4. Create the ignored `.env` once. This command copies the template and generates separate random passwords locally. Exclusive creation preserves any existing file. Set `DOCKER_DISK_DIR` in that file to the existing directory from step 3. Use forward slashes for Windows paths and point `ARTIFACT_DIR` to the intended artifact directory. Keep the passwords across restarts of the named volumes.

```powershell
.\.venv\Scripts\python -c "from pathlib import Path; import secrets; s=Path('.env.example').read_text(); f=open('.env','x'); f.write(s.replace('MYSQL_PASSWORD=\n','MYSQL_PASSWORD='+secrets.token_hex(24)+'\n').replace('MYSQL_ROOT_PASSWORD=\n','MYSQL_ROOT_PASSWORD='+secrets.token_hex(24)+'\n')); f.close()"
```

## Verify Phase 0 with real services

```powershell
.\.venv\Scripts\papersearch preflight
docker compose --profile '*' config --quiet
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verify-core.ps1
```

The script uses the dedicated Compose project `papersearch-m0`. It builds the CLI, waits for both health checks, runs an authenticated MySQL query and a Kafka metadata request in separate client containers, and checks Alembic's database connection. It then creates a three-partition test topic and recreates the two test containers using the same named volumes. Success requires the original MySQL server UUID and the saved Kafka topic. The output includes the applied CPU and memory limits. The process-level execution-policy switch applies only to this invocation. First use requires image and package downloads. With the images and built CLI cached, repeat the client checks offline:

```powershell
docker compose -p papersearch-m0 --profile core up -d --wait mysql kafka
docker compose -p papersearch-m0 run --rm --no-deps cli smoke mysql
docker compose -p papersearch-m0 run --rm --no-deps cli smoke kafka
docker compose -p papersearch-m0 --profile core stop
```

Clients on the Compose network use `mysql:3306` and `kafka:9092`. Both services have private container ports. From Windows, use the client commands above. The verification project retains its volumes after stopping.

## Workload profiles and storage

| Profile | Phase 0 use | Initial configured limit |
|---|---|---|
| core | MySQL and one KRaft Kafka broker | 768 MiB and 1 GiB respectively, 1 CPU each |
| test | One-shot CLI for independent client connections | 256 MiB, 1 CPU |
| snapshot | Leased fixture workers, Parquet and lexical package verification | 768 MiB, 1 CPU |
| batch | Local Spark history merge and CSV/Parquet analytics on isolated files | 3 GiB, 2 CPUs |
| embedding | Generate pinned CPU embeddings for eligible, leased papers | 2 GiB, 2 CPUs |

Run the heavy profiles separately. Spark and embeddings compete for the laptop's RAM and CPU. Kafka bounds its broker request queue at 64 and configures retention at 24 hours or 128 MiB per partition, with 16 MiB segments. Segment cleanup is asynchronous. Replay beyond this window uses the archived inputs.

Run `papersearch preflight --scale` from the host virtual environment before a scale workload. The reserve applies to free space on both the artifact filesystem and the host filesystem containing Docker's virtual disk. On a shared filesystem, the threshold is one combined working allowance. The initial scale allowance is 100 GiB. Revise it from measured data, temporary output and retained snapshots before a larger gate. Docker's internal free space and any virtual-disk cap must also be checked on the running engine. See the [verified M0 service results](docs/evidence/phase0-services.txt) and [earlier Windows setup checks](docs/evidence/phase0-windows.txt).
