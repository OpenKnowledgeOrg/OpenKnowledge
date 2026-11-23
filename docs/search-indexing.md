# Indexing papers into OpenSearch

```sh
uv run okp serve-index      # second serve process, on the OpenSearch VM
uv run okp search index     # queue the deployment and wait for its final state
```

Apply ClickHouse migration `00019` before starting the indexer. The `search-index/opensearch-vm`
deployment queues one run at a time, triggered by Completed or Failed runs of `s2-load`,
`s2-load-diff` and `s2-sync`, with a six-hour fallback schedule. It uses the shared Prefect
server (`PREFECT_API_URL`) and admission gate, and creates no S2 or ClickHouse concurrency
limits. Manual `okp search index` runs use the same deployment queue and need only the
shared Prefect connection locally; search credentials and settings belong to the OpenSearch VM.
On first installation the gate requires operator admission; see [deployment admission](deployment.md#deployment-admission).

## Documents and aliases

Readers query the `papers` alias. When `papers`, `abstracts`, `tldrs` and the read table
`paper_abstract_best` are applied at one release with matching live table UUIDs, and
`paper_abstract_best` was built from the loaded OpenAlex release (if any), the flow reads their
latest rows into one document per paper. If they are not aligned, it logs why and finishes without writes. Rows can be
newer than the pass's release: replacing merges discard older versions, so the indexer
always reads the latest state. The release bounds the key range and the diff's lower edge;
new IDs above the frozen range are picked up by the next diff. A contract
change or base table replacement builds a physical index named
`papers-v<contract>-<release>-<source fingerprint>`. After all parts succeed, the flow resets
refresh to its default, refreshes, and atomically switches the alias. Old indexes are retained.
Otherwise it updates the live index by ID for IDs touched after its recorded release,
covering missed releases in one pass, then refreshes and records the applied release.
A current run repairs the alias if needed and does no document writes.

Contract v3 takes `abstract` from `paper_abstract_best`: Semantic Scholar's abstract when it has
a live one, else that of the paper's chosen OpenAlex work, and `abstract_source` (`s2` or
`openalex`) says which. A work that several papers chose (`shared_work`) gives none of them an
abstract until the matching settles which paper it belongs to. Each new OpenAlex release builds a new physical index, since the release
is part of the name's fingerprint, and a pass whose OpenAlex release changes before it completes
is refused. Every document of an index built from OpenAlex release D carries at least version
`D * 2`, so a document never falls below a version it had. The DevTool must accept contract v3
before the alias moves to a v3 index.

## Mapping

`title`, `abstract` and `tldr` go through OpenSearch's `english` analyzer, which stems, so a match on `networks` finds `network`.
`publicationdate` is a `date` in `yyyy-MM-dd` that takes range queries and sorts, and a value in another shape leaves the field unindexed but keeps the document.
`title.exact` uses the `standard` analyzer without stemming.
The `lowercase` keyword normalizer makes filters on `venue`, `journal.name`, `s2fieldsofstudy.category`, `publicationtypes` and every mapped `externalids.*` ignore case; `_source` retains the original case.
`journal.volume` and `journal.pages` are keywords with their original case.
A keyword value longer than 8191 characters stays in `_source` but is not indexed, since OpenSearch refuses a term over 32766 bytes.
Changing the mapping means building a new index.
Parts are Prefect tasks with bounded concurrency. `OKP_INDEX_BASE_PARTS` defaults to 256,
`OKP_INDEX_DIFF_PARTS` to 36, and `OKP_INDEX_PARALLELISM` to 2. The roughly 255M papers
and 50M abstracts need about 1M papers per part: ClickHouse hashes a join side whole.
Diff joins read touched keys, so 36 parts target about 0.5M changed documents per part for a
weekly diff of 17–18M documents. Corpus ID distribution can be uneven; tune parts and
parallelism to the VM's memory. Each part streams into bulk requests of about 5 MB.
ClickHouse's `http_send_timeout` must allow for a stalled bulk request (30 seconds by default).
It is read from the server's default profile at startup; a per-query value has no effect.

## Resuming and failures

The `indexed` table stores frozen bounds, source UUIDs, contract version and completed parts.
A failed base leaves an unpublished partial index; a failed diff can leave some live documents
updated. Each part, the bounds/started receipt step and the closing step takes its own admission slot;
receipt and applied-state reads for planning take none.
Ordinary search exceptions release the slot, since requests are awaited and writes are idempotent.
Cancellation or uncertain admission still requires operator reconciliation; see [deployment admission](deployment.md#deployment-admission).
When a deploy closes admission, running parts drain and the next part is refused, failing the run.
Rerun after the deploy to skip successful parts. A changed part count replans every part of an
unfinished diff on the same index; that is safe, but replays the whole diff, so prefer changing
`OKP_INDEX_DIFF_PARTS` (or its default) between diffs unless parts keep failing. An unfinished base resumes at its own release
while contract, source UUIDs and part count match, even if applied has advanced. Once complete,
the next run diffs it to the applied release. No old index is deleted. Indexing holds no `clickhouse`
limit, so loads can continue. Rows changed during a read are sent again by the next pass.

Each document carries the largest `version` of its three rows, and the writes use `external_gte`, which accepts an equal version again.
Reading the latest rows keeps versions nondecreasing across resumed parts and later diffs.
The next diff re-reads keys touched after the receipt's release.
A bulk item that OpenSearch refuses with 429 is sent again up to 3 times, after 2, 4 and 8 seconds; an item still refused after that stops the pass with exit 1, and a rerun sends it again.
An index is created with `refresh_interval` at -1, so a base build does not write a small segment every second.
It needs `OKP_CLICKHOUSE_URL`, `CLICKHOUSE_PASSWORD`, `OKP_CLICKHOUSE_RAW_BUCKET`, `OKP_OPENSEARCH_URL` and `OPENSEARCH_PASSWORD`.
Unless set, `OKP_OPENSEARCH_USER` is `hub-indexer`, `OKP_OPENSEARCH_TIMEOUT_SECONDS` is 60 and `OKP_INDEX_SHARDS` is 5.
An unreachable OpenSearch or ClickHouse fails the flow; completed parts remain resumable.
`tests/test_search_e2e.py` runs it against ClickHouse and OpenSearch in Docker.

## Parallel indexing and worker upgrades

`OKP_INDEX_PARALLELISM` controls spawned partition workers (default 2, allowed 1–6).
Each process owns its ClickHouse/OpenSearch clients and handles one partition at a time;
only commands and results cross process boundaries. The coordinator owns admission,
completed-part receipts and final alias publication. Two workers leave CPU capacity for
OpenSearch; an eight-vCPU VM can use six workers. Increase from the conservative default
only after observing CPU and write queues.
The worker container remains bounded by its existing memory limit.

An image-only upgrade resumes the existing index: keep source table UUIDs, search contract,
`OKP_INDEX_BASE_PARTS`/`OKP_INDEX_DIFF_PARTS`, and persisted receipts unchanged. The stored
plan retains partition bounds; the image digest is not part of its identity. Do not reset the
index or rebuild source tables for this upgrade. Completed partitions are skipped and an
unfinished partition can replay its versioned writes safely.

Use the Infrastructure `deploy` workflow with the tested commit and immutable image digest.
It closes admission, waits for active partitions to finish, replaces both runners and reopens
admission. The old run can fail when it next attempts a partition while admission is closed.
After rollout succeeds, run `search-index/opensearch-vm` once (or follow an already queued
run). Verify that it uses the same physical index and advances the existing part receipts.
A drain timeout leaves admission closed: inspect the active work rather than clearing slots.
Lost worker processes also retain admission because server-side requests may still be running;
reconcile those requests before manually releasing retained slots. Cancellation waits for
submitted workers and retains admission under the existing interrupted-work policy.

For a network-free CPU comparison, run `uv run python -m tests.search_index_benchmark`.
A 40,000-document synthetic workload measured 4.42s inline versus 2.27s with two reused
workers (~1.95×) on the dev host. This measures validation/transformation/serialization,
not production end-to-end throughput; ClickHouse and OpenSearch can become the next limits.
