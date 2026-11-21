# PaperSearch reviewer verification

November 21, 2026. Base commit `0df94bb0bdcf423f003dfa6a4b903dee159204d8` with the M5 working tree. Windows 11, Docker Desktop Linux engine 29.9.3, Python 3.14.4, MySQL 8.4.13, Kafka 4.1.2, SQLite FTS5, PyArrow 23.0.1. The CPU-only reviewer uses FastAPI 0.165.0, Uvicorn 0.54.0 and Streamlit 1.67.0. [Runbook](../reviewer.md)

## Offline workflow and API tests

Command: `.\.venv\Scripts\python -u scripts/review_demo.py`. The fixture entered through an immutable archive, Kafka and the consumer before index publication. The test ran the actual Streamlit form logic with AppTest against live HTTP. It also checked the separately running UI server's health. The following output is from `artifacts/m5-verified-demo.log`.

```text
PASS Streamlit create/search/inspect/save fixture:river 1333edd9f4d19cd8fca86968bf45e22c97430cb2cb2b7603da4798face634592
PASS restart, concurrent idempotent retries, request collision, invalid inputs and project isolation
PASS newer metadata, older indexed text, reconsideration, history and snapshot rollover
PASS CSV export E:\PaperSearch\artifacts\papersearch-review-bd584539ff\decisions.csv
PASS offline reviewer demo papersearch-review-bd584539ff http://127.0.0.1:18501
```

The persisted label and note survived restarting MySQL, API and UI. Four simultaneous identical saves returned the same history ID. Four different simultaneous edits in a second project produced four history rows and the last committed edit became current. The first project's history remained intact. An invalid label or malformed project UUID returned 422. An unknown paper returned 404. A mismatched revision or a reused request ID with different content returned 409. The archived revision changed the abstract and advanced MySQL one generation. An older search hit still opened its original abstract, the UI displayed both warnings, and the export included the reviewed hash and new current hash. Publishing the next snapshot invalidated the earlier page token.

A separate headless Chrome check opened the served Streamlit page on port 18505, created a project, searched for `quantum`, verified the result linked to `https://arxiv.org/abs/0706.1228`, saved a note and downloaded the CSV through the displayed export link. The download contained the UUID of the project created in the browser and the exact note. This ran against the 10k snapshot while ingestion advanced. The local log is `artifacts/m5-browser-linked.log`.

## Baseline and regression checks

All 48 SHA-256 entries in the previous milestone's source inventory matched the checkout before edits. Reopening the previous fixture package and running its full validator confirmed 986 current papers, 986 lexical rows, 986 fixture vectors and zero semantic rows.

```text
.venv\Scripts\python -m pytest -q
30 passed, 4 subtests passed
.venv\Scripts\python -m ruff check src migrations tests scripts
All checks passed!
.venv\Scripts\python -m pip check
No broken requirements found.
scripts/verify-core.ps1       PASS M0 service checks
scripts/verify-storage.ps1    PASS M2 MySQL persistence and recovery (papersearch-m2-39c895fdb8c9)
scripts/verify-ingestion.ps1  PASS M3 Kafka recovery (papersearch-m3-cb161c730936)
scripts/verify-snapshots.ps1  PASS M4 snapshot publication and recovery (papersearch-m4-cadbb899f3a3)
```

## Real metadata selection

A range request for bytes 0 through 134217727 of Cornell arXiv v287 returned HTTP 206 and 134,217,728 bytes at 2026-11-21 19:46 UTC. Two independent extractions from this prefix produced byte-identical manifests (SHA-256 `dff1f198f17d0fe8f059ccad6c3eaa9f6ea5c24e844c3acaa029125cff354995`). The first 100,001 source lines yielded 100,000 distinct eligible papers. Selection excluded one row for invalid text. The selected batches contain 147,966,596 bytes. Dates and categories were unrestricted within the prefix. The capacity run used the metadata adapter and no PDFs. [Subset manifest](phase5-subset.json) · [Cornell data card and metadata license](https://www.kaggle.com/datasets/Cornell-University/arxiv/versions/287)

The real import used Compose project `papersearch-m5-real`, topic `papers`, consumer group `papersearch-ingest` and `D:\PaperSearch-M5` for archives, inputs and snapshots. The snapshot container was limited to one CPU and 768 MiB. It published 2,000 archived records at a time, then drained the consumer before the next batch. Search used the immutable FTS5 file with a 512 MiB API container limit. The publication and consumer logs are in `artifacts/m5-real.log`.


## Published real-data gates and serving measurements

| Selected | Current SQL papers | Lexical rows | Index on disk (MiB) | API RSS before load / after load / after queries (MiB) | Median HTTP query (ms) | Elapsed to verified publication (min) |
|---:|---:|---:|---:|---:|---:|---:|
| 10,000 | 10,000 | 10,000 | 16.8 | 112.7 / 116.0 / 117.1 | 77.4 | 5.1 |
| 50,000 | 50,000 | 50,000 | 83.3 | 112.6 / 115.9 / 117.6 | 79.0 | 34.7 |
| 100,000 | 100,000 | 100,000 | 166.3 | 112.9 / 116.1 / 118.5 | 275.4 | 74.3 |

Each gate had as many processed event markers as selected records, zero rejected selected records, zero derived jobs, and zero lag on all three partitions at publication. Semantic coverage was zero. An independent Windows pass validated the Parquet, SQLite and manifest checksums and compared every ordered paper ID and event ID with the selected raw input at all three gates. The final canonical corpus digest is `0d788a47831762a44ee8b5a93e2e3ba92adf172250dfc19647b712a7ae01d1eb`. Final partition offsets were 33,333, 33,468 and 33,199.

The first 50k builder exposed an expensive MySQL join. Its plan sorted an estimated 26,647 remaining rows before taking a 128-row page. The revised plan materialized 128 ordered IDs first and joined their JSON revisions. The original builder was stopped after the broker and MySQL had finished the 50k input. Twenty acknowledged batches were reopened on resume, with zero new publications and zero new consumptions. The old verified index remained available. The 50k and 100k snapshots used the revised query. The cumulative wall time includes this intervention and concurrent verification. At 100k, final acknowledgement, observed durable drain, and verified index visibility occurred at 20:54:25, 20:55:31 and 21:02:19 UTC. Overall throughput to verified publication was 22.4 papers/s.

The 100k index occupied 174,428,160 bytes. The API's first readiness request after process restart, including checksum verification, took 3.54 s. The five top-ten HTTP requests for `quantum` took 240.8, 275.4, 236.0, 471.0 and 372.2 ms. The API process high-water RSS over the measured load/query window was 118.5 MiB. The 100k importer/builder container reported a 139.0 MiB lifetime peak. The MySQL and Kafka directories measured 974.2 and 1,263.2 MiB respectively. API process RSS, container memory and WSL working set have different accounting. Service-directory samples were taken after publication and could include writes from the next gate. The raw reports retain individual queries, SQL allocation, disk free space, file hashes, container peaks, and WSL observations. [Gate measurements](phase5-measurements.json)

The actual 100k reviewer created a project and screened `arxiv:0811.2516`. Both Streamlit AppTest and a separate Chrome session used the deployed API. The first restart probe timed out after three seconds on the cold index. The corrected probe retries connection and read timeouts within a two-minute deadline. The repeated run passed restart, isolation, concurrency, pagination and export. Chrome downloaded a CSV containing its own newly created project ID and saved note. The saved sample below comes from the 100k API/UI workflow. The synthetic revision and reconsideration check used the isolated fixture so the real archive remains reproducible. [Sample decision export](phase5-decisions.csv)

```text
PASS Streamlit create/search/inspect/save arxiv:0811.2516 462d26dd8d975b4ec7e6d45b2d8bb9dfbef40e6475c2f8eba1dd91bcf2e07227
PASS restart, concurrent idempotent retries, request collision, invalid inputs and project isolation
PASS CSV export artifacts\m5-measure\decisions.csv
PASS Chrome browser project, search, real source link, save and downloaded CSV; https://arxiv.org/abs/0811.2516
```

Final logs on this machine: `artifacts/m5-real.log`, `artifacts/m5-real-resumed.log`, `artifacts/m5-paged-explain.log`, `artifacts/m5-serve-100k-final.log`, `artifacts/m5-browser-100k.log` and `artifacts/m5-reconcile.log`. After the paging change, real-service regression runs passed again in `papersearch-m2-0a0a2cf558cf`, `papersearch-m4-b15fa54286ea` and `papersearch-m3-3c2dedcdc6d1`.
