# Screen papers with PaperSearch

The reviewer runs locally. MySQL stores projects, decision history and the revision a reviewer actually saw. The API ranks titles and abstracts in the published SQLite FTS5 index. Title occurrences have weight 3 and abstract occurrences have weight 1. Search treats terms as alternatives, with indexed row order breaking ties. Restrict the matching field to title or abstract in the UI. The API returns the indexed event ID with each hit. If a different snapshot becomes active between pages, the UI offers to restart the search. [SQLite BM25](https://www.sqlite.org/fts5.html#the_bm25_function)

## Offline reviewer demo

For the retained 100k collection, the ranking selector also offers exact cosine search over its published semantic subset. The interface reports coverage, truncation and model fallback. [Open and verify semantic search](semantic-ranking.md)

From the Windows checkout with Docker Desktop running and the local MySQL passwords configured in `.env`, prepare the images and host test dependencies once with network access.

```powershell
.\.venv\Scripts\python -m pip install -r requirements-review.lock
docker compose --profile core pull mysql kafka
docker compose --profile snapshot --profile review build snapshot api
```

Run the demo from the same directory. It needs the local Docker engine and the prepared images. The fixture is already in the checkout.

```powershell
.\.venv\Scripts\python scripts/review_demo.py
```

Look for `PASS offline reviewer demo`. The script uses a fresh Compose project, saves its CSV in `artifacts/<project>/decisions.csv` and stops its containers. Add `--keep-running` to explore the fixture in a browser on port 18501. It serves the API on port 18000. Set `DEMO_UI_PORT` and `DEMO_API_PORT` to unused ports if needed. The demo prints its project name. To stop a retained run, use `docker compose -p <project> --profile core --profile review stop`.

1. In **Review projects**, create a project and enter its screening criteria.
2. Search for `river` in the fixture or `quantum` in the real dataset. Select a ranked result to read the abstract, source link and revision history.
3. Save a label and note. **Decision history** shows the reviewed revisions. When newer text appears, the interface flags the decision for reconsideration. A second reviewer project has its own decisions.
4. **Processing status** shows the current paper count, indexed and canonical generations, Kafka lag, derived jobs and separate lexical and semantic coverage.
5. **Export decisions (CSV)** downloads the project's current labels, notes, reviewed event IDs, reviewed/current text hashes and reconsideration flags. The API streams a consistent database view.

The script drives the Streamlit widgets with AppTest and the deployed HTTP API. It restarts MySQL, the API and UI, reopens the decision, races requests, imports a changed abstract through Kafka and checks the lagging index before publishing again. It checks HTTP status codes, history and CSV contents. The deployed UI also exposes a health endpoint. [Streamlit app testing](https://docs.streamlit.io/develop/api-reference/app-testing/st.testing.v1.apptest)

## Use a populated project

Use the Compose project and artifact directory that own the database and snapshots. From this laptop, the measured real collection can be opened with the following commands.

```powershell
$env:ARTIFACT_DIR = 'D:\PaperSearch-M5'
$env:REVIEW_API_PORT = '18805'
$env:REVIEW_UI_PORT = '18505'
docker compose -p papersearch-m5-real --profile core --profile review up -d --no-build --pull never mysql kafka api ui
```

Open the [local reviewer](http://127.0.0.1:18505) or the [local API documentation](http://127.0.0.1:18805/docs). These ports bind to loopback. On another checkout, the default ports are 8501 and 8000. Set `PAPERSEARCH_TOPIC` and `PAPERSEARCH_GROUP` to the topic/group used for your import. For an existing store outside `/artifacts/derived`, set `SNAPSHOT_ROOT` to its path inside the container.

## Reproduce the real-data gates

Choose a new artifact directory and Compose project for an independent run. Use a drive that passes the scale preflight for both the artifacts and Docker's data disk. The following PowerShell example assumes the pinned local images from the demo preparation.

```powershell
$env:ARTIFACT_DIR = 'D:\PaperSearch-reproduction'
$env:REVIEW_API_PORT = '18806'
$env:REVIEW_UI_PORT = '18506'
$project = 'papersearch-review-reproduction'
$python = '.\.venv\Scripts\python'
& $python -m papersearch.cli preflight --scale
if ($LASTEXITCODE) { throw 'Scale reserve unavailable' }
New-Item -ItemType Directory -Path $env:ARTIFACT_DIR -ErrorAction Stop
$prefix = Join-Path $env:ARTIFACT_DIR 'v287-prefix.part'
curl.exe --fail -sSL --range 0-134217727 -o $prefix 'https://api.kaggle.com/v1/datasets/download/Cornell-University/arxiv?datasetVersionNumber=287'
& $python scripts/select-review-subset.py $prefix (Join-Path $env:ARTIFACT_DIR 'selection')
if ($LASTEXITCODE) { throw 'Subset selection failed' }
docker compose -p $project --profile core up -d --wait mysql kafka
docker compose -p $project --profile snapshot run --rm --entrypoint alembic snapshot upgrade head
docker compose -p $project --profile review up -d --no-build api ui
$scripts = (Resolve-Path scripts).Path
foreach ($size in 10000,50000,100000) {
    & $python -m papersearch.cli preflight --scale
    if ($LASTEXITCODE) { throw 'Scale reserve unavailable' }
    docker compose -p $project --profile snapshot run --rm -v "${scripts}:/verify:ro" --entrypoint python snapshot -u /verify/verify-review-scale.py --target $size
    if ($LASTEXITCODE) { throw "Import or publication failed at $size" }
    & $python scripts/measure-review.py $project $size (Join-Path $env:ARTIFACT_DIR 'measurements')
    if ($LASTEXITCODE) { throw "Reviewer verification failed at $size" }
}
& $python scripts/reconcile-review.py $env:ARTIFACT_DIR
docker compose -p $project --profile core --profile review stop
```

The selector verifies the downloaded prefix checksum before extracting records. It takes the first 100,000 distinct IDs accepted by the arXiv adapter in source order, across all dates and categories. This supports reproducible capacity measurements. The archive contains 50 batches of 2,000 records each. Publication and consumption alternate so the input backlog stays bounded. Each gate records the acknowledged offsets, SQL counts, immutable snapshot and query result. The final reconciliation compares every selected ID and revision against the index and validates all snapshot files. The input manifest records each batch checksum and counts source rows excluded during selection. The data card assigns CC0 to the metadata and refers readers to individual licenses for papers. [Cornell dataset](https://www.kaggle.com/datasets/Cornell-University/arxiv/versions/287)

The scale output includes process and container peaks, on-disk index bytes, five individual HTTP timings, and a single capacity observation per gate. MySQL table allocation and service-volume bytes use different accounting. WSL memory includes Docker and any concurrent work. The source, snapshots, volumes and raw logs stay local. [Saved results](evidence/phase5-review.md)

The current snapshot, `91432e599277430688c2ab978babca2c`, serves 451,283 BM25 papers and 14,883 papers in semantic mode. Of the 351,283 recent CS/ML papers, 10,005 have a published mapping and 341,278 await encoding. The vision/language and statistical ML cohorts contain 10,004 distinct papers. An additional recent paper has identical model input and shares a vector. The reviewer also retains the original 4,878 historical mappings. Choose **semantic** in the **Ranking** selector to run these searches. [Live semantic results and mapping audit](evidence/reviewer-semantic.json) · [Measured batch timings](evidence/semantic-backfill.json)

## Publish BM25 over the complete Cornell v287 snapshot

Use the existing reviewer Compose project and the verified capacity input directory. The importer applies missing revisions in bounded transactions. A rerun skips recorded events. The publisher checks every indexed paper, revision and text hash against the full archive digest before switching the active snapshot.

```powershell
$env:ARTIFACT_DIR = 'D:/PaperSearch-M5'
$repo = (Resolve-Path .).Path
docker compose -p papersearch-m5-real --profile snapshot run --rm --no-deps -e PYTHONPATH=/workspace -v D:/PaperSearch-M8/input:/source:ro -v "${repo}/src:/workspace:ro" -v "${repo}/scripts:/tasks:ro" --entrypoint python snapshot -u /tasks/publish-full-arxiv.py /source --work-dir /tmp
```

After a completed import, add `--publish-only` to rebuild the index. Semantic search uses the compatible vectors already in the reviewer cache. The full BM25 build does not run an embedding worker.

The initial full BM25 snapshot `2af6901087c44a38be395e9e90ebc4ac` contained 3,050,173 metadata records, 3,050,173 BM25 papers and 14,883 semantic mappings.

Snapshot `c0e77a02d4d94f858ff8c35fe1adbeac` serves the same 3,050,173 BM25 papers and every one of the 486,123 papers eligible for semantic search. [Reproduce the full-policy backfill](semantic-ranking.md#backfill-every-paper-admitted-by-the-current-policy)
