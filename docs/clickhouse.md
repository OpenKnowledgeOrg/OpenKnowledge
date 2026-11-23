# Loading into ClickHouse

```sh
uv run okp s2 load --release <release_id> --dataset publication-venues --bucket openknowledge-508216-raw
```

It loads every object in the dataset's `manifest.json` into the dataset's table.
ClickHouse reads each object itself through its `raw` named collection and checks every record against the dataset's contract.
It needs `OKP_CLICKHOUSE_URL`, `CLICKHOUSE_PASSWORD` and `OKP_CLICKHOUSE_RAW_BUCKET`, the bucket that collection reads, but no S2 key.

`okp s2 load --measurement-report` optionally writes a new JSON report for that single
load. It distinguishes acknowledged inserts from skipped checkpoints and unknown partial
writes; memory values are snapshot gauges, not peaks. Use
`--measurement-kind synthetic-validation` for disposable development loads. Measurement errors are reported
separately and preserve the load outcome.

Each loaded object leaves a row in `loads`.
A rerun skips an object whose row matches its generation, its checksum, the table and the load SQL, so a table created again or emptied loads everything again.

## Migrations

Apply migrations 00011 to 00014 before starting loads with the new image. Inserts reject NULL into non-nullable columns. The shared insert safety setting changes upsert and delete SQL hashes for all seven supported datasets. Eligible base files for all seven and diff update/delete files for the six datasets with diffs replay when next loaded, including files already loaded with the previous image. Allow an idle window for the 00012 journal rewrite and these replays.
Migration 00014 orders citation versions by release, then deletion, then whether `contexts` is present, so a record with contexts beats the same record without.
A citations load refuses until 00014 is applied, and 00014 refuses while `citations` holds rows or checkpoints from the old order, whose merges may have dropped contexts.
To get past it, stop citation loads, run `RENAME TABLE citations TO citations_legacy`, run the Up statement of 00007 to create an empty table with a new UUID, apply the migrations and load the base release again from all its raw objects.
The old checkpoints no longer count and stay in place. Drop `citations_legacy` once that load passes its checks.

## Read tables

Migration 00015 adds `citation_edges_out` and `citation_edges_in`, slim tables ordered for
outgoing and incoming neighbourhood lookups. Migration 00017 adds papers_slim, narrow paper
metadata ordered by corpusid. The sync brings them current when papers or citations changes;
do it manually with `okp s2 build-read-tables` or the unscheduled Prefect
deployment `s2-build-read-tables/control-vm`. All three take the shared `clickhouse` limit.
When a source moved only by diffs since the release its read tables hold, they are updated in place:
the keys the diffs touched are deleted from them and inserted again from the source. Anything else
(a first build, a new base release, rows from before `citationid`) rebuilds all three, and so does
`--full`. A diff touching more than `OKP_READ_TABLES_MAX_DELTA_SHARE` of a read table's rows (0.1, the
most a full rebuild may drop) or more than `OKP_READ_TABLES_MAX_DELTA_KEYS` keys (100000000, which an
update holds in memory) rebuilds in full.
Before collecting delta keys, an in-place update requires 5 GiB plus 16 bytes per configured
maximum key (including the over-cap sentinel), about 6.5 GiB at the default cap. After collection,
it checks free disk again for the measured update rows plus 5 GiB. If it fails after its deletes,
the tables stay half-updated until a rerun finishes; their applied markers move only at the end.
The edge tables retain duplicate citing/cited pairs, so readers use `DISTINCT`, and they are plain
MergeTree, so a query with `FINAL` on them fails. `intents` is a mask of background (1),
methodology (2) and result (4); a null and an empty list both give 0, and other values are dropped.
Migrations 00018 and 00020 add `citationid` to `citation_edges_out` and `citation_edges_in` so readers can fetch citation contexts by primary key; existing rows read 0 until rebuilt after the migration.
When `citation_edges_out` has IDs but `citation_edges_in` does not, only `citation_edges_in` is rebuilt,
from `citation_edges_out`: after migration 00020, one plain build fills `citation_edges_in.citationid`.
A rebuild requires completed papers and citations releases and records each table’s source
release and live UUID in applied. A full rebuild needs about 28 bytes of free disk per citations row plus
a provisional 100 bytes per papers row. All three counts are validated before any swap; each
must retain at least 90% of its live table (an empty live table passes). A refusal leaves the
live tables unchanged; each sync repeats the rebuild until an operator resolves the refusal. A cancelled rebuild's
insert keeps running in ClickHouse; the next rebuild stops it and removes stale staging tables
before checking disk capacity. Apply migrations 00017 to 00020 before the first rebuild. After deploying,
run s2-build-read-tables once, verify all three applied markers at their source releases, and
delete the old s2-build-citation-edges deployment from Prefect.

## OpenAlex

Migration 00024 adds `paper_xwalk`, the OpenAlex works matching each live paper by DOI, arXiv ID,
PubMed ID, or MAG ID with the same normalised title and a year at most one apart, ranked so the
chosen work comes first. It is its own `openalex` group: it takes the papers diffs like
`papers_slim`, and a second applied marker, `paper_xwalk@openalex_works`, names the OpenAlex
release it was built from. A new OpenAlex release rebuilds only this group at the next sync or
`okp s2 build-read-tables`; while an OpenAlex load is unfinished the group keeps its last build.
Migration 00025 adds `paper_abstract_best` to the group: one abstract per live paper, the live S2
abstract when there is one (even an empty one), otherwise the non-empty abstract of the chosen
OpenAlex work, with `source` saying which. It takes the papers and abstracts diffs and holds the
earlier of their two releases; until abstracts holds a release, the group is skipped.
Migration 00026 makes `openalex_works.is_retracted` nullable: OpenAlex leaves it empty for some works,
and the load keeps that as NULL instead of refusing the file.
Migration 00053 adds the `title` match key (and `paper_abstract_best_invalidations` so weekly
search diffs rewrite papers whose title match is displaced by a new ID match). A full build of the
group then matches papers with no ID candidate to a work no paper chose, by equal normalised title
(20+ characters) and a year at most one apart, or a 40+ character title for a paper without a year.
The work must be the only eligible one in the paper's window and the paper the only live paper in
the work's; generic and serial titles, data deposits, datasets, paratext, peer reviews, errata and
retractions never match. An update matches by ID only: a paper it touches loses its title match
until the next full build. Roll back 00053 only after a build without title matches.
The `openalex-load` deployment builds the read tables right after its load, and the search index
follows each of its runs; a failed build leaves the load applied for the next sync to build from.
`openalex-release-check` (Mondays 04:00 Europe/Zurich) archives and loads OpenAlex's release until
both OpenAlex tables hold it, resuming a failed archive or load, and queueing behind a running
S2 sync. Its schedule stays off until
`OKP_OPENALEX_CHECK_SCHEDULE_ENABLED=true`: each new release downloads about 800 GB and
reindexes every paper.
Each OpenAlex table loads a release into `<table>_staging`, checks its row count there, and swaps
it in with `EXCHANGE TABLES`, so the live table always holds one whole release. Disk peaks at the
live tables plus one staging copy, dropped right after its swap; the database must be Atomic.
A copy left by a failed load is resumed by the next load of the same release, otherwise replaced.

## Serving, diffs and lineage

`tests/test_load_e2e.py` runs it against ClickHouse and a fake GCS in Docker.
On the control VM, `okp serve` serves the same flow as the deployment `s2-load/control-vm`, so a run can start from the Prefect UI.

`okp s2 load-diff --from-release <release_id> --release <release_id> --dataset <name> --bucket <bucket>` loads one archived diff, its update files as upserts and then its delete files as tombstones, and `okp serve` serves it as `s2-load-diff/control-vm`.
Both load deployments take the Prefect global concurrency limit `clickhouse`, which `okp serve` creates with a limit of 1, so no two served loads overlap, and each load deployment runs one run at a time, so a second run of the same load waits on the server and holds no slot.
The `applied` table records each base load and diff that passed its checks, and a diff loads only into a table that holds the release it starts from or the one it brings, which is a rerun.
When lineage is recorded, a base loads only into an empty table or one holding the same release, and `publication-venues` also into one holding an older release.
A load of `publication-venues`, which has no diffs, flags the venues its release no longer lists in `removed`.

## Watchdog

`okp serve` also serves `watchdog/control-vm`, which runs every 6 hours.
Each run writes `watchdog heartbeat`, then checks every table that follows diffs.
It writes one `watchdog stale` line per table whose last load was applied more than `OKP_WATCHDOG_MAX_AGE_DAYS` days ago, 14 by default.
A table never loaded does not count.
Stale data never fails a run, but a ClickHouse error does.
Both texts are fixed so that a log alert can match them.

Load recovery still needs persisted ownership of a begun load and shared coordination per table.
Until those are implemented, do not load a base over a populated table with missing lineage or overlap base and diff loads for the same dataset.
