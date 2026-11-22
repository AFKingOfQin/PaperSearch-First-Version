# File provenance

Python modules, tests and the fixture generator carry inline Codex attribution. The following files use JSON formats that do not support comments.

| Path | Origin and scope |
|---|---|
| `docs/evidence/phase4-manifest.json` | Copied byte-for-byte from the package published by the Codex-authored snapshot verifier on the 1,000-line synthetic fixture. Counts, file hashes and the publication ID are runtime output. |
| `tests/fixtures/phase1/cases.jsonl` | Codex authored the synthetic papers, revisions, duplicates, conflict pairs and deliberately malformed lines. |
| `tests/fixtures/phase1/expected.json` | Codex specified the expected winners, alternative revisions and rejection codes, then computed their contract digests separately from the application. |
| `tests/fixtures/phase1/arxiv-metadata.jsonl` | Codex selected and serialized eight bibliographic fields from Cornell snapshot v287, paper `0704.0001`. Their values match the source. The abstract is omitted. The complete source line, including LF, has SHA-256 `6c159128fb450c390e33b230b165e47cfe1a2a559f086e5ea7f9a882b5708cbb`. [Source dataset](https://www.kaggle.com/datasets/Cornell-University/arxiv/versions/287) |
| `docs/evidence/phase5-subset.json` | Runtime output of the Codex-authored selector, including the retrieval time, original input prefix hash, nested subset counts and per-batch hashes. |
| `docs/evidence/phase5-measurements.json` | Runtime reports from the real-data ingestion and reviewer measurement scripts, grouped by gate. |
| `docs/evidence/phase5-decisions.csv` | Runtime CSV export of a synthetic review decision on a real source record. The note and label are automated test inputs. |
| `docs/evidence/phase6-cpu.json` | Measured per-batch timings, token counts, input identities and process memory from the Codex-authored CPU benchmark. |
| `docs/evidence/phase6-evaluation.json` | Runtime output from the Codex-authored evaluator using three pinned public review tables and their original final inclusion decisions. |
| `docs/evidence/phase6-serving.json` | Measured HTTP and Streamlit results and independent cosine checks from the Codex-authored serving verifier. |
| `docs/evidence/phase6-snapshot.json` | Unmodified manifest from the CPU semantic package published against the retained 100k collection. |
| `docs/evidence/phase7-batch.json` | Runtime output from the Codex-authored batch verifier. Includes real source identity, deterministic synthetic update controls, input/output file hashes, laptop timings and reference equality checks. |
