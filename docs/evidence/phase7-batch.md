# Spark batch verification

**M7 and S2 passed.** In three trials, all six Spark merges agreed with their sequential references on 100,001 classifications: 10,000 new, 1,800 changed, 88,200 unchanged and one synthetic withdrawal. Every current record matched the original 100,000-paper corpus, SHA-256 `0d788a47831762a44ee8b5a93e2e3ba92adf172250dfc19647b712a7ae01d1eb`. CSV and Parquet matched the reference on every analytical row and all 149 category totals for 2008. There were 86,982 category memberships for that year, including 11,115 in astro-ph. [Reproduction commands](../spark-batch.md)

## Input and reference

On November 21, 2026, the retained M6 package passed the production validator against its recorded manifest hash. It contained 100,000 canonical/lexical records and 4,878 current semantic vectors. The 20 source and configuration files in commit `3e122e4619e08d70e0096e5b62db822377442ec1` reproduced the normalized source fingerprint recorded during M6. The Spark experiment reads that publication's Parquet export. It writes all modified inputs and results in a separate directory. [Source manifest](phase6-snapshot.json) · [Raw input and output manifests, settings and timings](phase7-batch.json)

The base contains the first 90,000 real paper IDs in published order. The next 10,000 real IDs arrive only in the updates. For the first group, deterministic row-number selection introduces 900 examples each of five cases: an earlier synthetic baseline updated to the real record, real replays, stale revisions, conflicting updates above an existing winner, and a new conflict that retracts a provisional base winner to an earlier real revision. The first case also permutes and repeats category membership to test normalization. One synthetic fixture paper loses its only unambiguous revision. Its withdrawal appears only in the change output. Every current final paper is an original unmodified arXiv record. This mixture measures processing under controlled updates; it gives no estimate of natural update frequency.

The sequential baseline streams sorted input parts with PyArrow. It groups revisions by paper and calls the existing Python revision oracle. The independent Spark implementation aggregates each priority and then performs a full outer join. The verifier compares each column of both Spark outputs with the sequential record for every ID, including previous and current event IDs, source precedence, both hashes, category arrays, titles and abstracts. It also reconstructs the published canonical corpus digest and checks every storage row and category count.

## What ran

Windows 11 on an Intel i7-12700H, Docker Desktop with Linux containers, Spark 4.1.1, Java 17 and a single local driver with two worker threads. Each measured application has a 1 GiB driver heap, a 3 GiB container limit and a two-CPU quota. The job sets four shuffle partitions, a 32 MiB input scan target and disables adaptive execution to keep plans comparable. The batch service has loopback networking only. The embedding worker was stopped. The timed trials followed the recovery checks.

One repetition consists of a host Python reference merge and a fresh Docker/Spark launch using the same input parts. The host timer for the reference starts before it opens the Parquet input and ends after it closes its partitioned Snappy Parquet output. The startup-inclusive Spark interval starts just before Compose launch and ends after the first merged Parquet write. The warmed interval repeats the entire merge in the same JVM, including a fresh scan and new output write. Neither implementation caches a DataFrame. OS filesystem caches were left in place. Input preparation, model downloads and post-run equality checks are outside the merge timers. Differences include the Windows native and Docker bind-mount I/O paths, ordering of the inputs and the execution engines.

For the storage comparison both writers receive the same six logical columns: paper ID, event ID, text hash, publication year, category membership and title/abstract text. Arrays and text are encoded as JSON values identically in both formats. Each timed query projects year and categories, filters to 2008, explodes category membership and returns counts ordered by category. Schema resolution, I/O and materialization of the grouped result are inside the query timer. The JVM is warm; query order alternates across runs. Byte totals count data parts and exclude filesystem checksums and success markers.

## Plans

The merge plan reads only identity and precedence fields on the base-winner side. On the final-winner side it scans the normalized base plus updates with full paper payloads. The exchanges partition records first by paper/version/time, then by paper. Spark sorts the winner streams by ID for the full outer sort-merge join. The writer hash-partitions by paper ID and sorts within each output part, which lets the verifier stream ordered records. The storage queries request only the two projected columns. A CSV reader must still traverse each text record, while Parquet can skip the unrequested column chunks. Both queries shuffle the category totals. [Physical plans captured from the job](phase7-plans.txt) · [Spark 4.1.1 tuning documentation](https://spark.apache.org/docs/4.1.1/sql-performance-tuning.html)

## Measurements

| Same merge, seconds | Trial 1 | Trial 2 | Trial 3 | Median |
|---|---:|---:|---:|---:|
| Sequential reference read through completed write | 3.254 | 2.967 | 2.961 | 2.967 |
| Spark launch through first completed write | 24.908 | 25.294 | 24.534 | 24.908 |
| Spark first merge after session creation | 20.858 | 21.064 | 20.289 | 20.858 |
| Spark warmed merge through completed write | 15.516 | 13.514 | 13.941 | 13.941 |

Spark took longer on every run, including the warmed merges. The reference could stream its already ordered inputs directly. Spark also paid for task scheduling, exchanges, sorting and writes through the bind mount. The timers cover those costs together. Peak Spark container usage ranged from 1.50 to 1.52 GiB. The largest observed Windows reference process working set was 453 MiB; this is a process-lifetime high-water mark and uses different accounting. Including all retained trial outputs, inputs, checksums, plans and logs, the scratch directory occupied approximately 1.27 GiB. This was a single laptop with two Spark threads.

| Storage dataset | Data bytes | Median write, 3 runs | Median selected-column query, 6 runs | Query range |
|---|---:|---:|---:|---:|
| CSV, UTF-8 with headers | 108,071,632 | 3.788 s | 1.844 s | 1.386–2.411 s |
| Parquet, Snappy | 64,128,353 | 6.732 s | 0.673 s | 0.637–1.316 s |

Parquet used 40.7% less storage and returned the projected category summary faster. Its writes took longer. Text and abstract bytes were present in both datasets and unneeded for this query, which favors a columnar scan. The comparison measures the specified encodings and compression settings. The result does not establish the same tradeoff for queries that need every field.
