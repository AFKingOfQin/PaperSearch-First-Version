# Clean-start verification, November 21, 2026 (US Central)

**M9 software handoff READY.** The test used a fresh source directory, virtual environment, credentials, MySQL and Kafka volumes, and snapshot directory. The fixture ran through the deployed HTTP API and the Streamlit widget test harness. The served UI health endpoint was checked. The saved CSV was subsequently re-exported from the restarted installation. [Operator runbook](../runbook.md) · [Final checklist](../ACCEPTANCE_CHECKLIST.md)

## Tested source and preparation

The base was `91b53d627a483cce3a01811c6fd41ce312db5d56`. These commands ran at the repository root. The archive contained all 119 tracked files. Only the pending demo script was overlaid for execution. Its SHA-256 was `d9352234676f4a0b31420e0b4e5500c7996f37f657acd41c3ac13cd04b03005e`. The other 118 exported files matched their archive bytes before placing the final handoff documents in the copy.

```powershell
git archive --format=zip --output=artifacts/m9-handoff/source.zip HEAD
py -3.14 -m zipfile -e artifacts/m9-handoff/source.zip artifacts/m9-handoff/source
Copy-Item scripts/review_demo.py artifacts/m9-handoff/source/scripts/review_demo.py
Set-Location artifacts/m9-handoff/source
py -3.14 -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements-dev.lock -r requirements-review.lock
.\.venv\Scripts\python -m pip install --no-build-isolation --no-deps -e .
.\.venv\Scripts\python -m pip check
docker compose --profile '*' config --quiet
docker compose --profile snapshot --profile review build snapshot api
```

The setup generated two new random passwords in an exclusively created `.env` and set the existing Docker disk directory. No old credentials or downloaded collections were copied. Both service images were cached by digest. Building the application images used package network access. Pip reported `No broken requirements found` and both application images built successfully.

The doctor reported Windows 11 build 26200, CPython 3.14.4, WSL 3.0.5.0 with the running docker-desktop distribution on WSL 2, Linux engine 29.9.3, Compose v5.7.0 and 16,626,065,408 engine RAM bytes. Core preflight passed on E: (22.60 GiB free) and Docker's C: drive (210.38 GiB free). The 100 GiB scale preflight correctly blocked E:. Running it with `ARTIFACT_DIR=D:/PaperSearch-M8` passed with 223.42 GiB free on D:. No scale import was needed.

## Reviewer, coverage and retained state

Docker's default address pools had already been allocated to retained projects. The isolated recovery suites used internal networks on `10.240.2.0/24` through `10.240.4.0/24`. The first demo try on an internal-only network produced `http://:0/ready` because this engine did not publish the port. The successful demo used an unused standard bridge with loopback ports. Host HTTP(S) proxies pointed to an unused loopback port, with localhost bypassed. The fixture used local services, packaged input and prebuilt images with pulling disabled. This test did not physically disconnect Windows.

```powershell
$env:COMPOSE_FILE = 'compose.yaml;../net5.yaml'  # standard bridge, 10.240.5.0/24
$env:STREAMLIT_BROWSER_GATHER_USAGE_STATS = 'false'
$env:HTTP_PROXY = 'http://127.0.0.1:9'
$env:HTTPS_PROXY = 'http://127.0.0.1:9'
$env:NO_PROXY = '127.0.0.1,localhost'
.\.venv\Scripts\python -u scripts/review_demo.py
```

Selected output from the successful run:

```text
PASS Streamlit create/search/inspect/save fixture:river 1333edd9f4d19cd8fca86968bf45e22c97430cb2cb2b7603da4798face634592
PASS restart, concurrent idempotent retries, request collision, invalid inputs and project isolation
PASS newer metadata, older indexed text, reconsideration, history and snapshot rollover
PASS CSV export E:\PaperSearch\artifacts\m9-handoff\source\artifacts\papersearch-review-0227139d84\decisions.csv
PASS fixture coverage: 1 metadata, 1 lexical, 0 semantic; 2 events, zero lag
PASS offline reviewer demo papersearch-review-0227139d84 http://127.0.0.1:18501
```

After revision, the current metadata and lexical generations both reached 2. All three partitions had zero lag. The fixture scheduled two derived jobs and ran no embedding worker. The exported decision retained the original event, changed from `relevant` to `uncertain`, and flagged the changed text. [API status](phase9-status.json) · [Exported decision](phase9-decision.csv)

After the automatic stop, the runbook's persistent-start command reopened the project using the same artifact directory and volumes `papersearch-review-0227139d84_mysql-data` and `papersearch-review-0227139d84_kafka-data`. The HTTP export matched all 456 saved bytes. The active snapshot ID and zero lag survived. All four services stopped again.

```text
PASS reopened export matches original bytes: 456 fixture:river uncertain 1
PASS reopened active snapshot: c71e03ea802e45ef96a3cf2256787a1b {'fixture_eligible': 1, 'lexical': 1, 'metadata': 1, 'semantic': 0, 'test_vectors': 0}
```

## Regression checks and audit

| Command in the clean source copy | Observed result |
|---|---|
| `python -m pytest -q` | 31 passed and 4 subtests passed in 2.79 s. The first restricted run could not use Windows' pytest temp directory. The rerun used normal user temp access. |
| `python -m ruff check src migrations tests scripts` and `python -m alembic heads` | All checks passed. Migration head was `0002_search_snapshots`. Windows blocked the generated Alembic launcher, so the documented command uses its Python module. |
| `powershell.exe -NoProfile -ExecutionPolicy Bypass -File ./scripts/verify-storage.ps1` | 62 PASS lines including M2, project `papersearch-m2-c6ff35c2b8fa`. |
| Same invocation with `verify-ingestion.ps1` | 52 PASS lines including M3, project `papersearch-m3-6c873626d314`. The before/after SQL crash runs matched baseline. |
| Same invocation with `verify-snapshots.ps1` | 41 PASS lines including M4, project `papersearch-m4-5e742c3f99cf`. Reopened 986 metadata and lexical rows, 986 fixture vectors and zero semantic vectors. |
| `python scripts/summarize-experiments.py verify` | Passed 27 controlled trials, 746 source blocks and both recorded capacity gates. This rechecks the evidence without repeating the full metadata import. |
| Production `snapshot_format.validate` on the retained D: publication followed by equality against `phase6-snapshot.json` | Reopened `a0c968b67725440e824d1ab9f2f3920c`. All 100,000 metadata and lexical rows, 4,878 current vectors and 3,506 truncated semantic inputs matched. [Original manifest](phase6-snapshot.json) |

Read-only `git log`, `git ls-files`, `git check-ignore` and content inspection found nine implementation commits from `df19df5` to `91b53d6`. All 80 comment-capable source, configuration and test files carried attribution. The comment-free inputs and generated outputs are listed in the [provenance manifest](../CODE_PROVENANCE.md). The largest tracked file was 163,183 bytes. No tracked file contained either configured local database password or a private-key header. The dataset, weights, volume, artifact, credentials and virtual-environment paths were ignored. The preexisting editor settings remained outside the handoff.

Full local logs are under `artifacts/m9-handoff/`. These SHA-256 values identify the exact transcripts used for the excerpts:

| Log | SHA-256 |
|---|---|
| `demo-final.log` | `F63C1169089577EF608DA55B8E5A9000C7ACB670E6847B5D1027F5F67D38F734` |
| `reopen.log` | `FCAFC73DA88120C3559D77D916B22813F665C0FA8112C1C68AD9E8953D2DB764` |
| `storage.log` | `AF2BAC477C6DD1A94186580D4D955C33C428E4110CC985B60C88DACD403EF18E` |
| `ingestion.log` | `2CDC1EA7B56F48F9A3045A33C2EE4EB83C49B0DFB44CABCCB56CC19A846DE218` |
| `snapshots.log` | `994BDAD18339ACF2A177A5DE6BAE766C4E1BA83F65B37E9BE4C0D5E5BD509C0B` |
