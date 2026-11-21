# MySQL storage and screening

## Import and review a paper

Run these PowerShell commands from the repository root after the Windows setup in the README. They use the default PaperSearch Compose project and the artifact directory shared with the CLI.

```powershell
.\.venv\Scripts\papersearch preflight
.\.venv\Scripts\python tests/fixtures/phase1/generate.py artifacts/phase1/development.jsonl
docker compose --profile core up -d --wait mysql
docker compose --profile test build cli
docker compose run --rm --no-deps --entrypoint alembic cli upgrade head
docker compose run --rm --no-deps cli db apply /artifacts/phase1/development.jsonl --format fixture --batch-id development --collected-at 2020-02-01T00:00:00Z
$corpus = docker compose run --rm --no-deps cli db export | ForEach-Object { $_ | ConvertFrom-Json }
$paper = $corpus | Where-Object paper_id -eq 'fixture:river'
$paper.payload | Format-List title,abstract
$project = docker compose run --rm --no-deps cli db project 'River screening' --criteria 'Studies of river flow' | ConvertFrom-Json
$request = [guid]::NewGuid().ToString()
docker compose run --rm --no-deps cli db decide $project $paper.paper_id $paper.event_id relevant --note 'Reports stream counts' --request-id $request
docker compose run --rm --no-deps cli db decision $project $paper.paper_id
docker compose --profile core stop mysql
```

The example uses the default host artifact path. If `ARTIFACT_DIR` points elsewhere, generate the fixture under that directory. Keep the request UUID when retrying a save. Generate a new UUID for an intentional edit. On a fresh database the export contains a generation header and 986 records. The example holds this small export in PowerShell memory. The CLI itself streams bounded pages.

## One transaction per valid event

`papers` gives each paper a stable identity and a row that writers lock first. After acquiring that lock, an importer checks the unique event ID in `processed_events`. A second delivery of a committed event returns immediately. For a new event, `paper_revisions` retains the normalized record, its source priority, text hash and archive reference. `current_revisions` points to the highest unambiguous priority. When two distinct revisions tie, both receive a conflict outcome. The paper falls back to the previous unambiguous revision, or disappears from the current export until the conflict is resolved by a later revision.

A change of current revision advances the corpus generation in `system_state`. For eligible text, it also inserts a job keyed by paper, text hash, kind, model revision and preprocessing identity. The revision, current pointer, generation, job and processed marker commit together. A failed transaction rolls all of them back, so a retry can start from the same state. Each malformed line gets a durable reason and archive location in `rejected_events`, keyed by its raw byte hash and input format. Database errors propagate to the caller for retry.

## Decisions and revisions

`review_projects` stores a project's topic and criteria. Each save appends a row to `screening_history` and updates the `screening_decisions` pointer for that project and paper in one transaction. A foreign key identifies the exact revision the reviewer read. A repeated request returns its original history ID even after a subsequent edit. Reusing its UUID with different content fails.

A decision read compares the reviewed title/abstract hash with the current hash. An author correction leaves the review current. Changing the abstract or withholding the paper sets `needs_reconsideration`. The old label, note and reviewed revision stay available to that project.

## Leases and exports

`derived_jobs` starts with embedding work for the synthetic fixture source. arXiv metadata is eligible for import and export. Selecting real papers for semantic backfill is a later scale decision. The job key includes the candidate model's immutable revision and preprocessing version `text-v1`. A metadata-only correction reuses the existing job. [Pinned model revision](https://huggingface.co/BAAI/bge-small-en-v1.5/tree/5c38ec7c405ec4b44b94cc5a9bb96e735b38267a)

Workers claim one row with `FOR UPDATE SKIP LOCKED`. MySQL sets the expiry using its own UTC clock. Each claim gets a fresh token. Completion or failure requires the current, unexpired token. A retryable error releases the job. A permanent error or three unsuccessful attempts leaves a failed job with a reason. The verification harness exercises the queue lifecycle with test workers. Model inference and artifact publication belong to later milestones. [MySQL locking reads](https://dev.mysql.com/doc/refman/8.4/en/innodb-locking-reads.html)

`db export` reads the generation and pages through current revisions in paper ID order inside one InnoDB repeatable-read transaction. A concurrent commit appears in the next export. The generation counts transitions, so different delivery histories can produce different generation numbers for identical final paper content. Compare the canonical record digest when checking replay across independent databases. [MySQL consistent reads](https://dev.mysql.com/doc/refman/8.4/en/innodb-consistent-read.html)
