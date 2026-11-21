# Semantic ranking verification, November 21, 2026

## Corpus and CPU cost

Before embedding, the Windows reconciliation rebuilt the expected ID/revision mapping from the retained Cornell arXiv v287 raw input. All 10,000, 50,000 and 100,000 ordered index rows matched at their respective gates. The 100k input snapshot was `e60d864780ed4644992eb5bf776e3232`, with corpus SHA-256 `0d788a47831762a44ee8b5a93e2e3ba92adf172250dfc19647b712a7ae01d1eb`. Windows scale preflight found 232.32 GiB free on D: and 242.67 GiB on the Docker host drive, above the 100 GiB reserve. [Source selection](phase5-subset.json)

Windows 11 build 26200, Python 3.14.4, Intel Core i7-12700H (14 cores, 20 logical processors), 31.73 GiB RAM, AC power. The Windows CPU run used ONNX Runtime 1.32.0 with the CPU provider, tokenizers 0.23.3, NumPy 2.5.5, two intra-op threads, one inter-op thread, tokenizer parallelism off, batch size eight and 128 tokens including special tokens. Model load took 1.334 s. Seed 54406 selected disjoint uniform samples without replacement from the 100k published corpus after 24 different warmup papers. Spark stayed stopped. A Docker image build overlapped part of the sustained interval. These observations cover one plugged-in laptop session. [Raw per-batch timings, input digests and run settings](phase6-cpu.json)

| Run | Texts | Mean / median / max tokens | Shortened | Wall seconds | Encode texts/s | Peak process working set |
|---|---:|---|---:|---:|---:|---:|
| Representative | 5,000 | 192.6 / 172 / 640 | 3,603 | 153.20 | 32.66 | 253.5 MiB |
| Sustained | 8,000 | 193.1 / 174 / 722 | 5,852 | 264.20 | 30.30 | 253.9 MiB |

Neither sample contained an empty abstract. Wall time includes SQLite reads, tokenization and encoding. The rate divides texts by the sum of measured encoding-call times. Peak working set is the process lifetime high-water mark at the end of each interval.

At 30.30 texts/s, encoding 100,000 texts would take about 55 minutes. A hypothetical 3.25 million would take 29.8 hours if the rate held. Float32 vector values alone take 146.5 MiB and 4.65 GiB respectively, before IDs, retained shards, the served matrix, database jobs or temporary and previous snapshots. The projections exclude fetching the model, durable writes, package building/validation and API startup. Longer workloads can experience different thermals and input lengths.

For this laptop increment the worker uses `CRC32(UTF-8 paper_id) mod 100 < 5`. That gives a repeatable 4,878-paper scope for end-to-end verification in minutes, 7.15 MiB of raw vector values and an estimated 161 seconds of pure encoding. It keeps a full-coverage lexical search available during publication. Embedding the remaining 95,122 papers and any larger collection is deferred. The hash samples stable IDs without using topics or labels. Its relevance to a particular search can vary substantially.

## Published index and recovery

The production worker ran for 338.6 s including model load, claims, encoding and durable writes. It completed all 4,878 jobs. Reconfiguring the same recipe and starting a second worker returned `{"claimed": 0}`. The 610 verified cache shards occupied 7,512,120 bytes. Snapshot construction and validation used a 128-paper page size, took 264.1 s and reached a cgroup peak of 100,167,680 bytes with a 768 MiB limit. The container timings include concurrent regression work.

Snapshot `a0c968b67725440e824d1ab9f2f3920c` contains the same corpus digest as the M5 source. The active-package verifier reread all 100,000 canonical records and lexical mappings and all 4,878 vector rows. All 4,878 eligible papers had current vectors, 3,506 were truncated and zero used titles alone. The semantic Parquet file was 14,686,089 bytes. Status showed all 4,878 jobs done, zero pending/leased/failed, no recent errors, and zero metadata generations behind. The manifest SHA-256 is `e8411898d955d8b0637337f127d44ccfbae0cdad4fb9d56e1536831c1445f3ef`. [Unmodified publication manifest](phase6-snapshot.json)

The deployed reviewer searched for `quantum information`. BM25's first request took 1,453 ms including cold lexical file verification. The first semantic request took 3,279 ms including loading model and matrix. Five subsequent semantic requests took 65.0, 46.3, 72.1, 64.0 and 69.4 ms (median 65.0 ms). The API cgroup peaked at 82,690,048 bytes before requests and 333,660,160 bytes after serving, against a 1 GiB limit. The isolated missing-model API returned the same results as BM25. Streamlit selected the same top event as the API and displayed actual coverage and truncation. A separate Windows matrix multiplication scored all 4,878 rows, reproduced the first four IDs and scores across two pages, and re-encoded the top passage from SQLite with a cosine of 1.0 against its stored Linux vector. [HTTP timings, first hits and independent results](phase6-serving.json)

The real-model isolation check terminated the worker immediately after acquiring a lease and again after its first durable batch. It published partial coverage of 1/2, restarted, and processed two more papers with one new computation and one verified cache hit. A missing model failed before a lease was taken. When a paper's text changed, publication omitted its previous vector until the new one was ready. The final fixture had three current vectors including one title-only record. A deliberately swapped event ID failed validation even after recalculating the file and manifest hashes. [Recovery check](../../tests/integration/semantic_recovery.py)

```text
{"claimed": 4878, "completed": 4878, "computed": 4878, "hits": 0, "shards_reused": 0}
PASS restart and cache reuse {'claimed': 2, 'completed': 2, 'computed': 1, 'hits': 1, 'shards_reused': 0}
PASS cross-file validation rejected a swapped revision with matching file checksums
PASS exact semantic ranking, stable pagination, indexed revision, truncation, Streamlit selection and model-unavailable BM25 fallback
```

## Review questions and evaluation protocol

The pinned ASReview conversion of Cohen et al. (2006) combines MEDLINE titles/abstracts with original OHSU drug-review decisions. The three operational questions concern comparative benefit and adverse effects for antihistamines in allergic rhinitis/urticaria, NSAIDs for arthritis pain, and triptans in acute migraine. The original reviewers considered drug, population, outcome, study design, duration, publication type and other eligibility criteria. The codebook distinguishes initial abstract screening from final article inclusion. PaperSearch used only the binary `label_included` for scoring. The texts and original decisions stay outside the repository. The report records the exact input URLs and SHA-256 hashes. [Pinned tables and provenance](https://github.com/asreview/synergy-dataset/tree/38b35218e4d0f99621cec5a8a25a0147bb88c654/datasets/Cohen_2006) · [OHSU triage criteria](https://dmice.ohsu.edu/cohenaa/systematic-drug-class-review-data.html) · [Original study](https://doi.org/10.1197/jamia.M1929)

The Drug Effectiveness Review Project reports describe the scope and comparison questions. Report updates provide context. Ground truth for these measurements comes from the frozen Cohen tables. [Antihistamine questions](https://pubmed.ncbi.nlm.nih.gov/21348042/) · [NSAID questions](https://pubmed.ncbi.nlm.nih.gov/20496448/) · [Triptan questions](https://pubmed.ncbi.nlm.nih.gov/21089254/)

Development used Antihistamines (310 records, 16 included, 23 title-only). Two query templates were specified in code. Mean development Recall@50 across the two rankers was 0.28125 for the topic alone and 0.40625 with the comparative-effectiveness/safety prefix. The latter was frozen for both held-out reviews. Each method received exactly the same complete stable-ID/title/abstract pool, including title-only records. BM25 uses the production SQLite weights (title 3, abstract 1) and orders unmatched documents by stable ID after matches. Cosine uses the production CPU encoder and exact scoring. Ties use stable order. Neither ranker receives labels, authors, keywords, full text or a judgment from the other ranker. Every ranked list, exact query and development result is retained. [Evaluation script](../../scripts/evaluate-rankers.py) · [Raw rankings and metrics](phase6-evaluation.json)

| Held-out review | Method | Precision@10 | Precision@20 | Recall@50 | Recall@100 | Query ms |
|---|---|---:|---:|---:|---:|---:|
| NSAIDs, 41 included / 393 | BM25 | .20 | .20 | 8/41 (.195) | 24/41 (.585) | 4.27 |
| NSAIDs | Cosine | .10 | .05 | 3/41 (.073) | 16/41 (.390) | 12.28 |
| Triptans, 24 included / 671 | BM25 | .00 | .00 | 5/24 (.208) | 8/24 (.333) | 6.22 |
| Triptans | Cosine | .00 | .10 | 6/24 (.250) | 11/24 (.458) | 10.92 |

These query times are single warm-model, local ranking calls. BM25 includes completion of the candidate ordering. Cosine includes query encoding and scoring. Encoding all candidate passages took 17.1 s for NSAIDs and 27.3 s for Triptans. There were 35 and 77 title-only records respectively. The serving test measures HTTP separately.

Both methods missed final inclusions and returned topical exclusions. In NSAIDs, BM25 surfaced record 256 (arthritis treatment costs), while cosine missed record 211 (a drug comparison reporting blood pressure and edema) in its first 100. In Triptans, cosine surfaced record 202 (a systematic review) and missed record 332 (a specific head-to-head trial) in its first 100. These descriptions identify observable differences in what ranked highly. The binary label alone does not give the individual reason for exclusion.

The two reviews favor different rankers. Precision and recall here measure recovery of historical final inclusions among already-collected candidates. Missing or truncated abstracts can omit eligibility details that full-text reviewers saw. These results establish neither completeness of a literature search nor effectiveness on the arXiv application corpus.
