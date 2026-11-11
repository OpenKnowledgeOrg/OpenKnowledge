from openknowledge_contracts.manifests import ObjectPath
from openknowledge_core.ingestion import (
    LoadDataset,
    LoadDatasetResult,
    OpenAlexLoad,
    OpenAlexLoadResult,
)
from openknowledge_core.ingestion.errors import TableRejected
from openknowledge_core.ingestion.read_tables import BuildReadTablesResult, ReadTablesPath
from openknowledge_pipelines.flows.openalex_load import check_release, load_then_build

RELEASE = "2027-01-13"
COMMAND = OpenAlexLoad(release=RELEASE, bucket="raw")


async def test_a_new_release_is_loaded_then_built_and_a_loaded_one_starts_nothing() -> None:
    events: list[str] = []

    async def load(command: LoadDataset) -> LoadDatasetResult:
        events.append(f"load {command.dataset} {command.release}")
        return LoadDatasetResult(
            release_id=command.release,
            dataset=command.dataset,
            table=command.dataset,
            objects=1,
            loaded=1,
            already_loaded=0,
            live_rows=1,
            identical_duplicates=0,
            removed=0,
            manifest=ObjectPath.under("raw", "openalex", command.release, "manifest.json"),
        )

    async def build() -> BuildReadTablesResult:
        events.append("build")
        if len(events) > 4:
            raise TableRejected("synthetic refusal")
        return BuildReadTablesResult(
            source_releases={"openalex_works": RELEASE},
            tables={},
            paths={"openalex": ReadTablesPath.FULL},
        )

    async def latest() -> str:
        return RELEASE

    async def loaded(release: str) -> bool:
        return release in taken

    taken: list[str] = []

    async def take(release: str) -> OpenAlexLoadResult:
        taken.append(release)
        return await load_then_build(load, build, COMMAND)

    first = await check_release(latest, loaded, take)
    assert events == [
        f"load {name} {RELEASE}" for name in ("sources", "works", "works-abstracts")
    ] + ["build"]
    assert first is not None and first.read_tables is not None
    # A loaded release starts nothing.
    assert await check_release(latest, loaded, take) is None and taken == [RELEASE]
    # A failed build leaves the load's result standing, with the reason.
    rebuilt = await load_then_build(load, build, COMMAND)
    assert rebuilt.read_tables is None and rebuilt.read_tables_failure == "synthetic refusal"
    assert rebuilt.works.release_id == RELEASE
