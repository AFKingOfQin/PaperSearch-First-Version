# Paper records and update events

`papersearch reference` reads UTF-8 JSON Lines files sequentially. It keeps distinct revisions in memory for small correctness comparisons. The result includes the current paper mapping, its checksum, conflicts and rejected lines. A completed scan exits successfully even when the report contains rejected records. An unreadable file or invalid batch configuration fails the command.

## Input formats

| CLI format | Mapping |
|---|---|
| `fixture` | Supply an unprefixed lowercase `id`, a title and offset-aware `source_updated_at`. The adapter prefixes the ID with `fixture:`. An omitted version becomes null. Missing abstract, authors and categories become an empty string or empty lists. Missing publication date and DOI become null. The default URL uses the reserved domain `example.invalid`. |
| `arxiv` | Read Cornell's bulk snapshot fields. Strip any arXiv prefix and version suffix, then prefix the stable ID with `arxiv:`. Legacy archive IDs retain the slash and omit the optional subject class. The abstract-page URL derives from that stable ID. Use structured author names in source order, or preserve the entire original byline as one string when structured names are absent. Split the category string on whitespace. |
| `event` | Read a v1 envelope with source, canonical paper ID, source version/time, collection time, batch ID, raw location/checksum and payload. The parser recomputes the supplied event, payload and embedding-text hashes. Unsupported schemas and mismatched hashes receive explicit rejection codes. |

Raw formats require an explicit batch ID and collection timestamp. The adapter records the input path and one-based line number, plus SHA-256 of the original bytes **including the line terminator when present**. An event envelope retains the original archive provenance across replay. Envelope parsing checks the raw checksum's format. Verifying it against an archive requires the original bytes.

For arXiv, the highest numeric entry in `versions` identifies the payload revision. An ID suffix must agree with this value. If there is no history, use the suffix if available. The `v1` creation date supplies the published date. For source update time, use the later of the selected version's creation instant and `update_date` at 00:00 UTC. Midnight represents the source's day precision. Either field is sufficient. A row with neither receives `missing_source_time`. Collection time never supplies revision precedence. [arXiv identifiers](https://info.arxiv.org/help/arxiv_identifier.html) · [Cornell metadata snapshot, version 287](https://www.kaggle.com/datasets/Cornell-University/arxiv/versions/287)

## Hashes and revision precedence

Normalize title, abstract, authors, categories and DOI to Unicode NFC, trim whitespace and collapse whitespace runs to one space. Preserve case and author order. Deduplicate and sort categories. Use UTC timestamps with six fractional digits and a trailing `Z`. Publication dates use `YYYY-MM-DD`.

All logical digests use SHA-256 of UTF-8 JSON with sorted object keys, compact separators and literal Unicode. Array order is significant.

| Digest | Input |
|---|---|
| `payload_hash` | Object containing normalized title, abstract, ordered authors, sorted categories, published date, source URL and DOI. |
| `event_id` | Array of schema version (integer 1), source, stable paper ID, source version, source update time and payload hash. Collection time, raw checksum, location and batch ID are provenance. |
| `embedding_text_hash` | Array containing `"text-v1"`, normalized title and normalized abstract. This identifies the text input. A future vector cache must also identify the model and its preprocessing. |
| `state_sha256` | The entire canonical paper mapping, including IDs, selected revisions, payloads and hashes. Delivery counters, raw provenance and quarantine outcomes are checked separately. |

For each paper, compare positive source versions first (unknown ranks below 1), then compare normalized source timestamps within a version. Identical events count as duplicate deliveries. When two distinct payloads share the same precedence, quarantine **all** variants at that precedence. Choose the newest remaining unambiguous revision. If every revision conflicts, omit the paper from the current mapping. Historical conflicts remain in the report after a newer valid revision arrives. Sorting the conflicting event IDs makes the report repeatable.

For example, `fixture:dispute` has a known v1 and two competing v2 titles, *North* and *South*. The output retains v1 regardless of which v2 arrives first. For `fixture:river`, changing only the authors preserves the text hash. Changing “Two streams.” to “Three streams.” changes it.

## Validation and fixture evidence

The reader limits each physical line to 256 KiB including its terminator. It drains larger lines in bounded chunks, hashes them and resumes on the next line. The adapters reject invalid UTF-8/JSON, duplicate JSON keys, unsupported schemas, invalid identifiers or timestamps, disallowed control characters, surrogate code points in text, and unsafe URL schemes. URLs are stored as metadata. Field limits include 4,096 title characters, 65,536 abstract characters, 4,096 authors and 256 categories. Rejected lines are keyed by raw SHA-256 with a stable reason code. Replaying a batch increases delivery counts and preserves distinct rejection identities.

The 18-line synthetic fixture has four explicitly expected current papers. The generator adds 982 distinct `fixture:sample-*` records. The expected manifest fixes both input checksums, full canonical state checksums, the four edge-case winners and every rejection/conflict. The expected winners and conflict groups were specified separately from the reference processor. The arXiv parser test uses a bibliographic projection of a real source row, kept separate from both synthetic fixtures. [Fixtures and expected outputs](../tests/fixtures/phase1) · [Provenance](CODE_PROVENANCE.md)
