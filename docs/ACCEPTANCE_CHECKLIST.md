# Final acceptance checklist

The software handoff and the owner's personal code review have separate sign-offs. The linked evidence travels with a source copy.

## Milestone evidence

- [x] The local progress ledger links M0–M9 to commands, outputs and delivered paths. Start with [M0 services](evidence/phase0-services.txt), [M1 contract](evidence/phase1-reference.txt), [M2 SQL](evidence/phase2-mysql.txt), [M3 Kafka](evidence/phase3-kafka.txt) and [M4 snapshots](evidence/phase4-snapshots.txt).
- [x] Application, ranking, batch, capacity and handoff evidence are available for [M5](evidence/phase5-review.md), [M6](evidence/phase6-ranking.md), [M7](evidence/phase7-batch.md), [M8](results.md) and [M9](evidence/phase9-clean-run.md).
- [x] Each attempted S1–S6 scale gate has exact achieved coverage and evidence or an explicit unresolved/deferred status. [Capacity evidence](evidence/phase8-capacity.json)
- [x] S1–S5 have measured outcomes. S6 remains deferred. The published reviewer has 100,000 lexical rows and 4,878 current vectors. The separate full-capacity database has 3,050,173 current metadata rows. [Results and scope](results.md)

## Repository and user workflow

- [x] A clean source export, fresh Python environment, new credentials and new persistent volumes reproduced the README workflow. [Setup log and reopened export](evidence/phase9-clean-run.md)
- [x] Service and base images are digest-pinned, Python requirements are locked, and the tested Docker and Compose versions are recorded. [Compose](../compose.yaml) · [Clean run](evidence/phase9-clean-run.md)
- [x] M0 evidence includes separate successful MySQL and Kafka client responses and the Linux engine's measured resource limits. [Service verification](evidence/phase0-services.txt)
- [x] Offline fixture imports and produces expected canonical records. [MySQL fixture comparison](evidence/phase2-mysql.txt)
- [x] A project can be created, searched, labeled, restarted, and exported. [Reviewer evidence](evidence/phase5-review.md)
- [x] Relevant/irrelevant/uncertain decisions and notes persist. [MySQL screening verification](evidence/phase2-mysql.txt)
- [x] Separate projects do not overwrite each other's decisions.
- [x] Revised abstracts retain decisions and show reconsideration status.
- [x] Active index snapshot and processing freshness are visible through the snapshot CLI. [Publication checks](evidence/phase4-snapshots.txt)

## Laptop and scale evidence

- [x] Actual WSL/Docker availability, limits, RAM, and disk are verified on the intended laptop. [Host and engine observations](evidence/phase0-services.txt)
- [x] The measured core, batch and embedding workloads completed within their documented limits. [Core service limits](evidence/phase0-services.txt) · [Spark resources](evidence/phase7-batch.md) · [CPU encoding resources](evidence/phase6-ranking.md)
- [x] The 100k application gate passes before larger expansion. [Reviewer evidence](evidence/phase5-review.md)
- [x] A representative 5k CPU embedding benchmark records settings, sustained rate, truncation, and peak RAM. [Raw 5k and 8k measurements](evidence/phase6-cpu.json)
- [x] 1m/full-snapshot metadata gates record exact counts, checkpoints, time, storage, and any blockers. [Capacity evidence](evidence/phase8-capacity.json)
- [x] Interrupted import/backfill resumes without losing decisions or verified completed work. [Ingestion recovery](evidence/phase3-kafka.txt) · [Embedding recovery](evidence/phase6-ranking.md)
- [x] Metadata, lexical, and semantic coverage are separately recorded and shown. [Reviewer evidence](evidence/phase5-review.md)
- [x] The backfill decision records achieved coverage and defers full-corpus encoding. [Scope and projection](evidence/phase6-ranking.md)
- [x] Index load/publication peak RAM and BM25 memory are measured. [Reviewer evidence](evidence/phase5-review.md)
- [x] Preflight checks the artifact and Docker disk filesystems. The operator budgets builds, temporary files and retained snapshots within the reserve. The clean-copy drive passed the core floor and correctly failed the larger scale floor. [Runbook](runbook.md) · [Observed preflight](evidence/phase9-clean-run.md)

## Systems correctness

- [x] Stable IDs/hashes and revision precedence are documented and tested.
- [x] Identical replay does not change canonical counts/state.
- [x] Stale revisions never overwrite current records.
- [x] Equal-priority conflicts have a deterministic explicit outcome.
- [x] Processed-event markers and metadata effects share a transaction. [Rollback and replay checks](evidence/phase2-mysql.txt)
- [x] Offset commits follow durable outcomes and contiguous partition progress. [Kafka fault and offset logs](evidence/phase3-kafka.txt)
- [x] Crash tests before/after the database commit pass.
- [x] Database failure does not skip valid inputs.
- [x] Malformed records are durably quarantined.
- [x] Abandoned derived jobs recover through leases.
- [x] Cache keys include content, model revision, and preprocessing. [Cache and crash checks](evidence/phase4-snapshots.txt)
- [x] Only validated complete packages become active. Previous packages remain readable during interrupted builds.
- [x] Concurrent decision writes and metadata updates preserve history.
- [x] Kafka ingestion recovery results equal the uninterrupted reference result. [State and label comparisons](evidence/phase3-kafka.txt)

## Data, Spark, and ML evidence

- [x] Real dataset sources, licenses, sampling, checksums and retrieval dates are recorded. [Input provenance](results.md) · [Ranking pool identities](evidence/phase6-evaluation.json)
- [x] Synthetic fixtures/modified events are explicitly distinguished from real metadata. [Experiment report](results.md)
- [x] Tracked-file inventory and ignore checks cover local credentials, environments, large inputs, volumes and weights. [Audit](evidence/phase9-clean-run.md) · [Ignore rules](../.gitignore)
- [x] Spark merges base and update revision histories and matches all reference records and classifications. [Batch reconciliation](evidence/phase7-batch.md)
- [x] CSV/Parquet comparisons use identical logical data and queries. [Physical plans](evidence/phase7-plans.txt) · [Raw measurements](evidence/phase7-batch.json)
- [x] Full versus incremental experiments end with identical canonical results. [Experiment report](results.md)
- [x] Timing endpoints, cold/warm behavior, repetitions, and hardware are recorded. [Experiment report](results.md)
- [x] Actual slower/failed configurations and resource limits are reported. [Experiment report](results.md)
- [x] BM25 and embedding evaluation use the same labeled candidate pools. [Pool hashes and rankings](evidence/phase6-evaluation.json)
- [x] Evaluation labels are withheld from the rankers. [Evaluation implementation](../scripts/evaluate-rankers.py)
- [x] Dataset inclusion labels and title/abstract limitations are explained. [Evaluation protocol](evidence/phase6-ranking.md)
- [x] Ranking claims are limited to the evaluated candidate pools, and recovery claims cover local processes and retained disks. [Limitations](architecture.md) · [Ranking protocol](evidence/phase6-ranking.md)

## Technical handoff

- [x] The [README](../README.md) introduces the problem and gives the demo command.
- [x] The offline verification uses local services and the packaged fixture. [Commands and expected state](runbook.md) · [Observed results](evidence/phase9-clean-run.md)
- [x] The [architecture and limitations](architecture.md), [data dictionary](data_dictionary.md) and [runbook](runbook.md) cover the delivered system.
- [x] The [CS544 mapping](cs544_mapping.md) links concepts to source paths, tests and measured results.
- [x] Results report references raw measurements and source commits. [Experiment report](results.md)
- [x] Read-only inspection found nine successive implementation commits from setup through the capacity benchmark. The owner handles commits. Git records the changes and authorship metadata, not who operated the keyboard. [History audit](evidence/phase9-clean-run.md)
- [x] Git inspection and source export for this handoff were read-only. The index was empty at the start and at final verification. [Audit](evidence/phase9-clean-run.md)
- [x] AI-generated source/config/test files carry attribution. Comment-free fixtures and measured artifacts have provenance entries. [Manifest](CODE_PROVENANCE.md) · [Audit](evidence/phase9-clean-run.md)
- [ ] The user has reviewed the code they will explain.

## Blockers

The remaining checkbox belongs to the project owner. Walk through one duplicate delivery, one interrupted publication and one saved decision using the [code walkthrough](architecture.md), then mark your personal review complete. This is a human sign-off after software verification.
