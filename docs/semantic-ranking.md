# CPU semantic search

The reviewer can rank papers by terms shared with the query (BM25) or by the cosine similarity of learned representations of the query and each paper. The original 100,000-paper benchmark published 4,878 vectors selected by a stable hash of each paper ID. The current policy also admits every paper first submitted since January 1, 2024 with any `cs.*` or `stat.ML` category. The historical sample remains eligible and uses the same vector cache. The interface shows the current lexical and semantic counts, how many eligible vectors are ready, how many inputs were shortened and how many used only a title. Both modes open the same revision-aware review and screening controls. [Measurements and evaluation](evidence/phase6-ranking.md) · [Current collection and reproduction](reviewer.md)

The retained reviewer serves 14,883 semantic paper mappings to 14,882 distinct cached vectors. Of those papers, 10,005 are recent (2024-01-01 through 2026-05-21) and 4,878 belong to the 2007–2008 sample. The 341,278 remaining eligible papers have durable pending jobs. [Live ranks and checksums](evidence/reviewer-semantic.json) · [Backfill and publication timings](evidence/semantic-backfill.json)

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
docker compose -p $project --profile embedding run --rm --no-deps embedding snapshot embed --batch-size 8 --max-jobs 256
docker compose -p $project --profile snapshot run --rm --no-deps snapshot snapshot build
docker compose -p $project --profile snapshot run --rm --no-deps snapshot snapshot verify
docker compose -p $project --profile snapshot run --rm --no-deps snapshot snapshot status
```

The general worker leases up to eight jobs by default. Ingestion and backfill both admit recent CS/ML papers and the original 5% paper-ID sample. Scoped backfill schedules current winners from a published collection and leases just those jobs. Repeating a completed slice skips its completed jobs. It checks stored shards before using a cached vector. The key covers the title/abstract content hash, job kind, model name, revision and preprocessing version. Completed jobs survive restart. An abandoned lease becomes available on expiry. Run `snapshot embed` again to resume. Inspect failed job IDs with `snapshot status`. After repairing the cause, use `snapshot retry-semantic JOB_ID` for a terminal failure and rerun the worker. Publication can include a partially completed subset. It counts only artifacts matching the current text and recipe, validates file hashes, row IDs, paper IDs, revision IDs and vector norms, then switches the active snapshot.

Each passage is the stored title, a newline and its abstract. A missing abstract uses the title alone. Queries receive the model card's English retrieval instruction. Both use the pinned tokenizer, the first 128 tokens including special tokens, and a preserved final separator. The model's maximum is 512. The shorter input bounds inference cost and can lose information late in an abstract. ONNX Runtime uses its CPU provider, two intra-operation threads and one inter-operation thread. The encoder takes the first token's 384-dimensional representation and L2-normalizes it. The API caches one loaded model and one exact float32 matrix per process. A new snapshot replaces the matrix on the next semantic request.

## Expand the recent collection

The first laptop release covers all papers tagged **both** `cs.CL` and `cs.CV` from 2024 through the last available submission, plus every `stat.ML` paper from calendar 2024. These overlapping groups cover vision/language and statistical learning. Selection uses the original submission date and all category tags. A revision can enter the scope on a category change even when its abstract stays the same. The UI reports actual vectors, eligible papers still pending, dates, and counts within each group. Its category counts include cross-listings.

With the retained project and environment above, rerun the first measured pilot as follows. The lower date is inclusive and the upper date is exclusive. Repeat `--category` to require every named category. The command checks the published corpus checksum, verifies each current revision in MySQL and prints counts and elapsed seconds. Each batch commits separately. A restart can repeat the same command.

```powershell
docker compose -p $project --profile embedding run --rm --no-deps embedding snapshot backfill --start 2024-01-01 --before 2024-04-01 --category cs.CL --category cs.CV --batch-size 16
```

The remaining measured slices use the following arguments with the same command and batch size.

| Start (inclusive) | Before (exclusive) | Required categories |
|---|---|---|
| 2024-04-01 | 2025-01-01 | `cs.CL` and `cs.CV` |
| 2025-01-01 | 2026-01-01 | `cs.CL` and `cs.CV` |
| 2026-01-01 | 2027-01-01 | `cs.CL` and `cs.CV` |
| 2024-01-01 | 2024-07-01 | `stat.ML` |
| 2024-07-01 | 2025-01-01 | `stat.ML` |

To extend coverage, choose another calendar slice and category, such as `stat.ML` for the first quarter of 2025. Compare the reported counts, seconds and available resources before increasing the range. Future ingestion automatically queues papers admitted by the broader policy. A newly queued paper becomes searchable after encoding and publication.

```powershell
docker compose -p $project --profile snapshot run --rm --no-deps snapshot snapshot build --page-size 1024
& $python scripts/check-semantic-coverage.py http://127.0.0.1:18805 artifacts/semantic-repeat.json
```

## What the live queries returned

| Semantic query | VL-Cache rank | LOOK-M rank |
|---|---:|---:|
| VLM KV Cache | 2 | 3 |
| Compressing the attention key and value cache to reduce memory during inference in vision-language models | 1 | 49 |
| How can an image-and-text transformer use less memory for the attention keys and values it saves during generation? | 506 | 166 |

Chart question answering, object hallucination in captions, conformal prediction intervals, causal discovery and Bayesian optimization each returned a relevant first result. The less specific phrase `detecting invented objects in image descriptions` returned a patent-image retrieval paper first. These wording-sensitive results use the same pinned BGE model and exact cosine scorer. [Ranked papers and scores](evidence/reviewer-semantic.json)

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

For the original **100,000-paper benchmark**, the historical verifier expects that exact collection. With that benchmark reviewer running, verify both HTTP ranking modes and the missing-model fallback against a second local API process.

```powershell
docker compose -p $project --profile review run -d --rm --no-deps --name papersearch-m6-no-model -p 127.0.0.1:18816:8000 -e PAPERSEARCH_MODEL_DIR=/absent api
& $python scripts/verify-semantic-review.py http://127.0.0.1:18805 http://127.0.0.1:18816 (Join-Path $env:ARTIFACT_DIR 'serving-repeat.json')
docker stop papersearch-m6-no-model
docker compose -p $project --profile core --profile review stop
```
