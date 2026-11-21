# CPU semantic search

The reviewer can rank papers by terms shared with the query (BM25) or by the cosine similarity of learned representations of the query and each paper. The published arXiv collection contains 100,000 BM25-searchable papers. A stable hash of each paper ID selects 4,878 for semantic search. The interface shows both counts, how many eligible vectors are ready, how many inputs were shortened and how many used only a title. Both modes open the same revision-aware review and screening controls. [Measurements and evaluation](evidence/phase6-ranking.md)

## Open the retained reviewer

Run in PowerShell from the repository root with the existing local `.env`, artifacts and Docker volumes.

```powershell
$env:ARTIFACT_DIR = 'D:\PaperSearch-M5'
$env:REVIEW_API_PORT = '18805'
$env:REVIEW_UI_PORT = '18505'
$project = 'papersearch-m5-real'
docker compose -p $project --profile core --profile review up -d --no-build --pull never --wait mysql kafka api ui
```

Open [the local reviewer](http://127.0.0.1:18505), select a project, search for `quantum information` and switch **Ranking** between **bm25** and **semantic**. Semantic mode scores all published vectors, with stable corpus row ID breaking ties. BM25 remains available over the entire lexical index. If the model files are unavailable or the snapshot has no supported semantic index, the API returns BM25 results and a reason that the UI displays. Requests pinned to a superseded snapshot still prompt the reviewer to restart pagination.

## Install the local model and build from an existing M5 collection

Use the artifact directory and project that own the M5 database. To reproduce from an empty collection, complete the [real-data setup](reviewer.md) first. Prepare these dependencies once with network access. The model and data live outside Git. The runtime loads only local files and checks both model file hashes on startup. The model is BAAI/bge-small-en-v1.5, revision `5c38ec7c405ec4b44b94cc5a9bb96e735b38267a`, licensed under MIT. [Pinned model files and model card](https://huggingface.co/BAAI/bge-small-en-v1.5/tree/5c38ec7c405ec4b44b94cc5a9bb96e735b38267a)

```powershell
$python = '.\.venv\Scripts\python'
& $python -m pip install -r requirements-review.lock -r requirements-embedding.lock
$model = Join-Path $env:ARTIFACT_DIR 'models\bge-small-en-v1.5'
New-Item -ItemType Directory -Force $model | Out-Null
$base = 'https://huggingface.co/BAAI/bge-small-en-v1.5/resolve/5c38ec7c405ec4b44b94cc5a9bb96e735b38267a'
foreach ($file in 'onnx/model.onnx','tokenizer.json','README.md') {
    curl.exe --fail -sSL "$base/$file" -o (Join-Path $model (Split-Path $file -Leaf))
    if ($LASTEXITCODE) { throw "Download failed for $file" }
}
docker compose -p $project --profile embedding --profile review --profile snapshot build embedding api snapshot
& $python -m papersearch.cli preflight --scale
if ($LASTEXITCODE) { throw 'Scale preflight failed' }
docker compose -p $project --profile core up -d --wait mysql
docker compose -p $project --profile embedding run --rm --no-deps embedding snapshot configure-semantic
docker compose -p $project --profile embedding run --rm --no-deps embedding snapshot embed --batch-size 8
docker compose -p $project --profile snapshot run --rm --no-deps snapshot snapshot build
docker compose -p $project --profile snapshot run --rm --no-deps snapshot snapshot verify
docker compose -p $project --profile snapshot run --rm --no-deps snapshot snapshot status
```

The worker leases up to eight jobs at a time. New imports and backfill use the same rule, `CRC32(UTF-8 paper_id) mod 100 < 5`. It checks stored shards before using a cached vector. The key covers the title/abstract content hash, job kind, model name, revision and preprocessing version. Completed jobs survive restart. An abandoned lease becomes available on expiry. Run `snapshot embed` again to resume. Inspect failed job IDs with `snapshot status`. After repairing the cause, use `snapshot retry-semantic JOB_ID` for a terminal failure and rerun the worker. Publication can include a partially completed subset. It counts only artifacts matching the current text and recipe, validates file hashes, row IDs, paper IDs, revision IDs and vector norms, then switches the active snapshot.

Each passage is the stored title, a newline and its abstract. A missing abstract uses the title alone. Queries receive the model card's English retrieval instruction. Both use the pinned tokenizer, the first 128 tokens including special tokens, and a preserved final separator. The model's maximum is 512. The shorter input bounds inference cost and can lose information late in an abstract. ONNX Runtime uses its CPU provider, two intra-operation threads and one inter-operation thread. The encoder takes the first token's 384-dimensional representation and L2-normalizes it. The API caches one loaded model and one exact float32 matrix per process. A new snapshot replaces the matrix on the next semantic request.

## Reproduce the checks

The isolated recovery check exercises process interruption, lease expiry, verified cache hits, text changes, empty abstracts and corrupted row mappings using the real model. The original fixture reviewer demo also runs with cached images and no model download.

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-embeddings.ps1
& $python scripts/review_demo.py
```

For the CPU benchmark, stop other heavy workloads. Use the 100k source snapshot shown below for the retained project, or substitute the 100k snapshot ID from your own M5 reconciliation. To rerun the label evaluation, download the three pinned review tables once. The evaluator checks each SHA-256 before ranking. Write new measurements into the local artifact directory.

```powershell
$index = Join-Path $env:ARTIFACT_DIR 'derived\snapshots\e60d864780ed4644992eb5bf776e3232'
& $python scripts/benchmark-embeddings.py $index $model (Join-Path $env:ARTIFACT_DIR 'cpu-repeat.json')
$pools = Join-Path $env:ARTIFACT_DIR 'review-pools'
New-Item -ItemType Directory -Force $pools | Out-Null
$source = 'https://raw.githubusercontent.com/asreview/synergy-dataset/38b35218e4d0f99621cec5a8a25a0147bb88c654/datasets/Cohen_2006/output/local'
foreach ($name in 'Antihistamines','NSAIDS','Triptans') {
    curl.exe --fail -sSL "$source/$name.csv" -o (Join-Path $pools "$name.csv")
    if ($LASTEXITCODE) { throw "Download failed for $name" }
}
& $python scripts/evaluate-rankers.py $pools $model (Join-Path $env:ARTIFACT_DIR 'ranking-repeat.json')
```

With the retained reviewer running, verify both HTTP ranking modes and the missing-model fallback against a second local API process.

```powershell
docker compose -p $project --profile review run -d --rm --no-deps --name papersearch-m6-no-model -p 127.0.0.1:18816:8000 -e PAPERSEARCH_MODEL_DIR=/absent api
& $python scripts/verify-semantic-review.py http://127.0.0.1:18805 http://127.0.0.1:18816 (Join-Path $env:ARTIFACT_DIR 'serving-repeat.json')
docker stop papersearch-m6-no-model
docker compose -p $project --profile core --profile review stop
```
