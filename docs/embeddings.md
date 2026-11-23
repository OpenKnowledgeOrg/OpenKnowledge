# SPECTER2 vector lookup

Converts the archived `embeddings-specter_v2` base or diff into flat Float32 files in the curated bucket, with a ClickHouse index from corpus ID to file and row. Vectors stay in GCS.

```
okp embeddings convert --release <release_id> --raw-bucket <raw> --curated-bucket openknowledge-508216-curated
okp embeddings load-index --release <release_id> --raw-bucket <raw> --curated-bucket openknowledge-508216-curated
okp embeddings lookup --curated-bucket openknowledge-508216-curated --id <corpusid>   # or --ids-file FILE
```

Diffs go one hop at a time, in release order, on top of the loaded base; anything else is refused.
A diff needs two `convert` runs (`--from-release <id> --part updates`, then `--part deletes`) and one
`load-index --from-release <id>`, which loads both parts.

`convert` dispatches one Cloud Batch job (label `embeddings-convert`), one task per archive shard;
each task runs `okp embeddings convert-shard` and needs no S2 key. Tasks run `OKP_BATCH_PARALLELISM`
at a time (8 on the control VM); set it high enough for the run. `convert` blocks until its job ends
(up to `OKP_BATCH_WAIT_TIMEOUT_SECONDS`, 24 h) and holds the S2 slot meanwhile, so a scheduled
s2-sync is refused as busy and alerts. A job orphaned by a killed `convert` blocks Semantic Scholar
archive downloads until it ends; OpenAlex jobs are not affected.

- Objects: `embeddings/specter_v2/<release>/<part>/<shard stem>.f32` and `.index.parquet`; `<part>`
  is `base`, `updates` or `deletes`. A diff's sit under `<release>/from-<from_release>/<part>`. A deletes shard has only an index.
- Format: little-endian Float32, 768 values per row, no header. Row `r` is bytes
  `[r*3072, (r+1)*3072)`, readable with one range request. A corpus ID repeated inside a shard keeps
  its last row.
- Index: table `paper_embedding_locations` (migration `00052`), `ReplacingMergeTree(version, is_deleted)`
  on `corpusid`. A paper both updated and deleted in one diff ends up deleted.
  `load-index` then writes one `applied` row; before that, lookup refuses with "no
  embedding index loaded".
- `load-index` prints `repeated`: corpus IDs repeated across shards of one release, plus papers both
  updated and deleted in one diff.
- Lookup: at most 20,000 IDs per call, each vector returned as its 3,072 raw bytes (about 60 MB at
  20,000), 64 concurrent range reads, no retry; a GCS error aborts the
  call, rerun it. Its ClickHouse queries time out after `OKP_CLICKHOUSE_READ_TIMEOUT_SECONDS` (60 s),
  not the load timeout, and it needs no raw bucket. Missing and deleted IDs are both reported missing.
- If `load-index` fails partway, rerun it; reloading is safe. A different base needs a new table
  (drop it and create it from migration `00052`): a truncated table keeps its `applied` record, so
  a base into it is refused. While a diff loads, and after one fails until the rerun, a lookup of a
  paper with rows of that diff is refused, and so is any other load. A diff rerun must start from
  the release the recorded load started from.
- One curated bucket holds a base and all its diffs: lookup reads every row from the bucket it is
  given. One raw archive per release: outputs are named by release, part and shard, not `--prefix`.
- Do not delete older release prefixes under `embeddings/specter_v2/` or put them under a lifecycle
  rule: after a diff, rows still point into the base and every earlier diff.
- ClickHouse grants: `load-index` needs `SELECT, INSERT` on `paper_embedding_locations` and
  `applied`; lookup needs `SELECT` on both. Neither needs a grant on `system.tables`. A caller such
  as DevTool must validate corpus IDs itself (non-negative, fits UInt64) before calling lookup.

Prerequisites outside the code: on `openknowledge-508216-curated`, the Batch service account needs
object create, get and delete (a rerun overwrites), and the control VM service account needs list
and get. Infra grants none of this yet. Reading it from DevTool needs a further read grant.

Throughput and cost are unmeasured.
