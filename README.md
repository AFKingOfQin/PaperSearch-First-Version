# PaperSearch

PaperSearch will ingest revisions to paper metadata and preserve a reviewer's screening decisions across updates and restarts. Phase 0 provides the CLI, migration scaffold, and local service verification. The reviewer demo is a later milestone. See [project status](PROGRESS.md) and the [offline demo procedure](docs/DEMO_SCRIPT.md).

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
.\.venv\Scripts\alembic heads
.\.venv\Scripts\papersearch doctor
```

Expect 5 GiB and 100 GiB in the configuration output. Alembic starts with an empty revision history. The doctor reports host RAM, disk, WSL distributions and Docker engine limits. It exits with code 1 when a prerequisite is missing. The commands above work offline once dependencies are installed.

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
| batch | `docker compose --profile batch run --rm spark` prints the Spark runtime version | 3 GiB, 2 CPUs |
| embedding | `docker compose --profile embedding run --rm embedding` prints the CPU worker's Python version | 2 GiB, 2 CPUs |

Run the heavy profiles separately. Spark and embeddings compete for the laptop's RAM and CPU. Both commands are environment probes until their implementation phases. Kafka bounds its broker request queue at 64 and configures retention at 24 hours or 128 MiB per partition, with 16 MiB segments. Segment cleanup is asynchronous. Later ingestion needs archived inputs for replay beyond this window.

Run `papersearch preflight --scale` from the host virtual environment before a scale workload. The reserve applies to free space on both the artifact filesystem and the host filesystem containing Docker's virtual disk. On a shared filesystem, the threshold is one combined working allowance. The initial scale allowance is 100 GiB. Revise it from measured data, temporary output and retained snapshots before a larger gate. Docker's internal free space and any virtual-disk cap must also be checked on the running engine. See the [verified M0 service results](docs/evidence/phase0-services.txt) and [earlier Windows setup checks](docs/evidence/phase0-windows.txt).
