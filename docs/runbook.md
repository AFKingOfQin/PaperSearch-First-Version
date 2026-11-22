# Run PaperSearch from a clean source copy

Use Windows PowerShell in the root of a checkout or extracted source archive. The fixture ships in the repository. Initial provisioning needs CPython 3.14 x64, Docker Desktop running its WSL 2 Linux engine, and access to the pinned packages and images. Subsequent fixture runs use the local images and never pull. The demo needs neither paper downloads nor model weights.

## Provision once

```powershell
py -3.14 -m venv .venv
$py = '.\.venv\Scripts\python.exe'
& $py -m pip install -r requirements-dev.lock -r requirements-review.lock
& $py -m pip install --no-build-isolation --no-deps -e .
& $py -m pip check
& $py -m papersearch.cli doctor
```

Create the local credentials once per installation. The following command uses exclusive creation. Keep the same credentials with the database volume.

```powershell
& $py -c "from pathlib import Path; import secrets; s=Path('.env.example').read_text(); f=open('.env','x'); f.write(s.replace('MYSQL_PASSWORD=\n','MYSQL_PASSWORD='+secrets.token_hex(24)+'\n').replace('MYSQL_ROOT_PASSWORD=\n','MYSQL_ROOT_PASSWORD='+secrets.token_hex(24)+'\n')); f.close()"
```

In `.env`, set `DOCKER_DISK_DIR` to the existing host directory containing Docker Desktop's disk image, shown under **Settings → Resources → Advanced**. Use forward slashes. The demo creates its own artifact folder under this checkout. The core reserve defaults to 5 GiB. Before a large import, point `ARTIFACT_DIR` to the measurement drive and run preflight with `--scale`. Its 100 GiB default is a free-space floor. Budget separately for simultaneous archives, temporary builds, retained snapshots and Docker's virtual-disk cap. [Measured volume growth](results.md)

```powershell
& $py -m papersearch.cli preflight
docker compose --profile '*' config --quiet
docker compose --profile core pull mysql kafka
docker compose --profile snapshot --profile review build snapshot api
```

If the two digest-pinned service images are already cached, the pull is optional. Rebuild the application images after an application or dependency change.

## Verify the offline reviewer

```powershell
.\.venv\Scripts\python -u scripts/review_demo.py
```

Look for `PASS offline reviewer demo`. The script starts new MySQL and Kafka volumes, applies the migrations, publishes a synthetic river paper, and saves a decision through Streamlit and the API. It restarts MySQL, the API and UI, checks the original history, edits the label, and imports a changed abstract. It tests reconsideration against the older snapshot and publishes the new one. The final status must show **1 current paper, 1 lexical row, 0 semantic rows, 2 processed events, 0 rejections and 0 broker lag**. The final CSV should have one decision for `fixture:river`, labeled `uncertain`, with different reviewed/current text hashes and reconsideration set to 1.

Each run prints a unique project name and puts `status.json` and `decisions.csv` in `artifacts/<project>/`. Services stop at the end and the volumes persist. To explore interactively, add `--keep-running` and open the printed loopback URL. Choose the existing review project and inspect its history, status and export. To stop it, use the printed project name in `docker compose -p <project> --profile core --profile review stop`. [Reviewer controls and real-data setup](reviewer.md)

To reopen a **previously stopped** demo, run the following from the same checkout in a separate PowerShell session. Supply the exact name printed by the successful run. Reuse its Compose override if you supplied one, and substitute its ports if you changed them.

```powershell
$project = Read-Host 'Printed Compose project name'
$env:ARTIFACT_DIR = (Resolve-Path "artifacts/$project").Path
$env:PAPERSEARCH_TOPIC = 'papers'
$env:REVIEW_API_PORT = '18000'
$env:REVIEW_UI_PORT = '18501'
docker compose -p $project --profile core --profile review up -d --no-build --pull never --wait mysql kafka api ui
Invoke-RestMethod http://127.0.0.1:18000/status
```

Open `http://127.0.0.1:18501` and select the saved review project. When finished, stop the services from the same shell.

```powershell
docker compose -p $project --profile core --profile review stop
```

## Diagnose or repeat a check

| Observation | What to inspect or run |
|---|---|
| A port is occupied | Set `DEMO_API_PORT` and `DEMO_UI_PORT` in PowerShell to two unused loopback ports before launching. |
| Docker reports exhausted address pools | Inspect existing subnets with `docker network inspect --format '{{.Name}} {{range .IPAM.Config}}{{.Subnet}}{{end}}' @(docker network ls -q)`. Supply an unused subnet as below. Each retained test project needs a distinct range. |
| Broker lag is zero but search is behind | Inspect `/status` for the active generation, job counts, recent errors and recipe. Use the correct Compose project, database and artifact directory, then verify or build the package. [Snapshot commands](snapshots.md) |
| A save or import was interrupted | Retry a save with its original request UUID. Resume ingestion with the same archive, Kafka group and database. Check durable outcome counts and committed offsets. [Decision semantics](storage.md) · [Replay steps](ingestion.md) |
| Semantic search is unavailable | Inspect published coverage, the pinned model files and the current recipe. The fixture supports BM25. [Semantic runbook](semantic-ranking.md) |

For an exhausted address pool, choose a private subnet that is unused by Docker, the host and any VPN. This PowerShell example writes the override under the ignored artifact directory. A standard bridge lets Docker Desktop publish the reviewer ports.

```powershell
$subnet = '10.240.50.0/24'  # replace with an unused range
New-Item -ItemType Directory -Force artifacts | Out-Null
Set-Content artifacts/demo-network.yaml "networks:`n  default:`n    ipam:`n      config:`n        - subnet: $subnet" -Encoding ascii
$env:COMPOSE_FILE = 'compose.yaml;artifacts/demo-network.yaml'
.\.venv\Scripts\python -u scripts/review_demo.py
Remove-Item Env:COMPOSE_FILE
```

Run the host tests and retained-evidence verifier independently of the demo. The three service suites create separate projects and exercise SQL persistence, Kafka crash boundaries and interrupted snapshot publication.

```powershell
.\.venv\Scripts\python -m pytest -q
.\.venv\Scripts\python -m ruff check src scripts migrations tests
.\.venv\Scripts\python -m alembic heads
.\.venv\Scripts\python scripts/summarize-experiments.py verify
foreach ($suite in 'storage','ingestion','snapshots') {
    powershell.exe -NoProfile -ExecutionPolicy Bypass -File ".\scripts\verify-$suite.ps1"
    if ($LASTEXITCODE) { throw "$suite recovery failed" }
}
```

The handoff evidence uses an equivalent clean source copy: a read-only `git archive` of the source commit extracted into an empty directory, with the listed pending demo change copied in before testing. The local environment, credentials, artifact store and service volumes were created there. [Commands and observed output](evidence/phase9-clean-run.md)

## Share the project

The configured destination is [AFKingOfQin/PaperSearch](https://github.com/AFKingOfQin/PaperSearch). When you choose to publish, review and commit the handoff files manually, push your reviewed branch to that remote, and share the commit permalink with the runbook. The recipient can reproduce the fixture without the laptop's private volumes.
