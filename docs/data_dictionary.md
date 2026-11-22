# PaperSearch data dictionary

| Durable record | Identity and meaning |
|---|---|
| Archived input and manifest | Original bytes, batch identity, format, collection time and checksums allow replay after broker retention expires. A source timestamp orders revisions independently of delivery time. |
| `papers`, `paper_revisions` | One stable, namespaced paper ID can have many event IDs. Each revision retains normalized JSON, source priority, text and raw-line hashes, and the batch and location needed to trace it. |
| `current_revisions` | At most one winning event ID per paper. Joining to the exact revision yields the current title, abstract and bibliographic fields. A paper with no unambiguous revision has no row here. |
| `processed_events`, `rejected_events` | A unique valid event records its applied, stale or conflict outcome. A rejected line is keyed by raw-byte hash and format and retains its reason and location. Repeated deliveries and unique outcomes have different counts. |
| `system_state` | Singleton generation changes when canonical state changes. Compare it with the active manifest generation to measure indexing lag. |
| `review_projects` | UUID, review topic and screening criteria. The same paper can have independent decisions in different projects. |
| `screening_history`, `screening_decisions` | Each save records project, paper, reviewed event, request UUID, time, label and note. The current pointer selects one history row per project and paper. Labels are relevant, irrelevant or uncertain. |
| `derived_jobs` | Unique paper, text hash and recipe. Status is pending, leased, done or failed. Attempts, last error, lease token and expiry support inspection and recovery. |
| `derived_artifacts` | Unique cache entry per text hash and recipe. A shard checksum, slot and vector checksum identify immutable bytes reusable across papers. |
| `derivation_state`, `published_snapshots` | Store identity, recipe, epochs and active snapshot pointer. Each publication records a manifest checksum and the generation and artifact epoch it represents. |

The two migrations define column types, keys and constraints. The record contract defines normalized text, event identity and tie handling. [Initial schema](../migrations/versions/0001_paper_state.py) · [Publication schema](../migrations/versions/0002_search_snapshots.py) · [Record contract](record-contract.md)

In a package, `corpus.parquet` maps each row to a paper and indexed event, their hashes, title, abstract, authors, categories, publication date, source URL and DOI. `lexical.sqlite` indexes the same ordered rows. `semantic.parquet`, when present, carries vectors and their row identity. Fixture digest bytes in `test-vectors.parquet` count separately from model vectors. The manifest declares metadata, lexical and semantic counts and hashes each file. [Exact Arrow schemas and validation](../src/papersearch/snapshot_format.py)

The decision CSV exports the reviewed event and text hashes alongside the current revision information and reconsideration flag. A downstream reviewer can see which text justified a decision after the indexed abstract changes. [Sample export](evidence/phase5-decisions.csv)
