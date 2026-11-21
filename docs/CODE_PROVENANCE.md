# File provenance

Python modules, tests and the fixture generator carry inline Codex attribution. The following files use JSON formats that do not support comments.

| Path | Origin and scope |
|---|---|
| `tests/fixtures/phase1/cases.jsonl` | Codex authored the synthetic papers, revisions, duplicates, conflict pairs and deliberately malformed lines. |
| `tests/fixtures/phase1/expected.json` | Codex specified the expected winners, alternative revisions and rejection codes, then computed their contract digests separately from the application. |
| `tests/fixtures/phase1/arxiv-metadata.jsonl` | Codex selected and serialized eight bibliographic fields from Cornell snapshot v287, paper `0704.0001`. Their values match the source. The abstract is omitted. The complete source line, including LF, has SHA-256 `6c159128fb450c390e33b230b165e47cfe1a2a559f086e5ea7f9a882b5708cbb`. [Source dataset](https://www.kaggle.com/datasets/Cornell-University/arxiv/versions/287) |
