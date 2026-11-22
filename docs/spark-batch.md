# Spark batch merge

The batch job answers two questions for each paper. Which revision was current in the base history, and which is current after the updates? It joins the two answers by paper ID, writes the complete winning record, and labels the change. The online reviewer continues to use its published snapshot. The batch container runs Spark in local mode without a network connection or database credentials. [Measured results](evidence/phase7-batch.md)

## Run the offline contract check

From the configured Windows checkout with Docker Desktop running, fetch the pinned Spark image once. The Python environment uses the existing project dependencies. Spark and Java are supplied by the image.

```powershell
docker compose --profile batch pull spark
.\.venv\Scripts\python scripts/verify-spark-fixture.py
```

The test converts the checked-in fixture with the normal event adapter, runs a real Spark job and compares the result with the independently pinned fixture digest. It keeps its small output in a fresh directory under `artifacts/`.

## Merge your own normalized histories

Pass two Parquet paths containing the fields in the project's normalized corpus schema, excluding the snapshot-specific `row_id`. Input identities and hashes must come from validated event records. The merge accepts a directory of partition files for each input. Each record represents one revision. If the base has previously seen conflicting or superseded events, supply its full distinct revision history. A current-only snapshot loses older alternatives that a later conflict could restore. A current-only base suffices for the measured arXiv split because M5 imported one unique event per paper. [Record contract](record-contract.md) · [Input schema](../src/papersearch/batch/reference_merge.py)

```powershell
$env:BATCH_DIR = 'D:\PaperSearch-batch-inputs'
docker compose --profile batch run --rm --no-deps --pull never spark --master 'local[2]' --driver-memory 1g --conf spark.sql.shuffle.partitions=4 --conf spark.ui.enabled=false /jobs/spark_merge.py /batch/base /batch/updates /batch/result
```

The job expects the output path to be new. Read a completed output after the command exits successfully and `report.json` exists. A failed attempt can leave partial files in its private directory. Use a fresh output path on retry.

Within each paper, the job groups records by source version and normalized UTC update time. Identical event IDs at a priority are replays. Distinct IDs at the same priority disqualify that priority. The highest remaining version wins, breaking ties by timestamp and treating an unknown version as zero. It sorts and deduplicates category membership. A full outer join of base and final winners yields `new`, `changed`, `unchanged`, or `withdrawn`. A paper with no unambiguous winner on either side has no current record or transition. Withdrawing a base winner retains its prior event ID in the change output. A metadata-only revision can be changed while retaining its text hash.

## Reproduce the 100k comparison

Use the verified M6 snapshot and a **new** scratch directory on a drive with the scale reserve. Stop embedding and ingestion workloads for the timing trials. The following PowerShell commands write new raw measurements to the scratch directory.

```powershell
$python = '.\.venv\Scripts\python'
$env:ARTIFACT_DIR = 'D:\PaperSearch-M5'
& $python -m papersearch.cli preflight --scale
if ($LASTEXITCODE) { throw 'Scale reserve unavailable' }
$snapshot = Join-Path $env:ARTIFACT_DIR 'derived\snapshots\a0c968b67725440e824d1ab9f2f3920c'
$run = 'D:\PaperSearch-M7\repeat'
& $python scripts/prepare-spark-input.py $snapshot docs/evidence/phase6-snapshot.json $run
if ($LASTEXITCODE) { throw 'Input preparation failed' }
& $python scripts/measure-spark.py $run 1 2 3
if ($LASTEXITCODE) { throw 'Inspect the trial log in the scratch directory' }
& $python scripts/check-spark-results.py $run (Join-Path $run 'verified.json') (Join-Path $run 'plans.txt') 1 2 3
```

Preparation verifies the source package and creates sorted parts for both paths. The sequential baseline streams those parts and calls the same priority-selection function as the project's reference processor. Spark uses its own DataFrame implementation. Each trial writes one reference result, two Spark results and two equivalent storage datasets. Verification streams all five sets of files. It checks each merged field, revision, hash and classification, reconstructs the original 100k corpus digest, compares every CSV/Parquet value, and checks the category summary against an independent Python count. The run retains the inputs, outputs and plans for inspection.
