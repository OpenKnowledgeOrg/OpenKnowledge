# Deployment

## Pipelines image

`pipelines/Dockerfile` builds one image for Batch download tasks, loads and the Prefect worker.
Its entry point is `okp`; the worker runs `prefect` from the same image.
Build it from the repository root:

```sh
docker build -f pipelines/Dockerfile -t pipelines .
docker run --rm pipelines s2 download-shard --help
```

Pull requests build and smoke-test it.
Every push to `main` publishes it as `europe-west4-docker.pkg.dev/openknowledge-508216/openknowledge/pipelines:<commit>` and writes the digest to the job summary.
Deployments pin that digest, never a tag, and pass it as `OKP_IMAGE_DIGEST`: receipts record it, and an image cannot know its own digest.

## Deployment admission

All mutating CLI commands and served flows hold a shared Prefect
`deployment-admission` slot for their lifetime, except search indexing, which admits each part
and its closing step separately. Read-only `read-base`,
`verify-base-manifest` and watchdog work while closed. Existing admitted Google Batch
shard workers finish without Prefect access; direct shard commands require admission.

Fresh `okp serve` creates the gate closed; restarts preserve it. Install this prerequisite
in an idle window with old clients disabled: existing binaries do not honor admission.
Never delete/recreate, deactivate or enable decay on the gate while clients can run.

- `okp admission status` prints `{"supported": true, "closed": bool, "active_slots": int}`.
- `okp admission close` sets capacity to zero, refusing new work while admitted work finishes.
- `okp admission open` reopens after deployment verification; it refuses while slots are held.
- `okp admission release --slots N` frees all held slots, only while closed and when `N`
  matches the held count. First reconcile every run and external job using admission owner
  logs, Prefect run states, Batch operations and ClickHouse activity (`system.processes`,
  checked as the ClickHouse container default user, not the `openknowledge` user).
  Never release to bypass drain.

Sync holds the `semantic-scholar` lease from before admission through the whole cycle,
including ClickHouse loads; managed downloads refuse while it runs. Batch recovery still
precedes each source operation.
Standalone loads, load-diff and citation edge rebuilds (CLI and served flows) acquire `clickhouse`
before admission and fail promptly when busy, without capacity retries. Sync waits for
ClickHouse while holding its source lease and admission; it does not refuse on load contention.
Source lease refusals or cancellation before admission take no admission slot.
Except for ordinary search exceptions, only successful work releases its slot.
Failed/interrupted runs and uncertain API responses
require operator reconciliation: external work may survive. Counter mutations are never
replayed on uncertain responses. This includes a load of a release that has not been archived,
even if validation failed before writes: close admission, verify no surviving work, and use
`admission release` before reopening. Loads require the archived manifest and objects first.
Closing alone does not establish idleness.
A scheduled sync or load starting while closed ends Failed and mails `run_failed`;
a gate left closed for days mails at every scheduled run.
