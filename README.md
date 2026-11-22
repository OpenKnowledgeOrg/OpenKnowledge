# OpenKnowledge

OpenKnowledge is a free, open-source tool that helps researchers understand the literature
around a topic, find the papers relevant to their work, and see how ideas emerge, connect
and evolve over time.

> **Status:** under active development. This repository currently holds the data foundation
> the platform is built on. The platform's core features will move here from the
> [prototype](https://github.com/OpenKnowledgeOrg/OpenKnowledgeMVP).

## Core features

- **OK-Score.** A relevance score the researcher can tune. It weighs more than citations, for
  example open access, released code and public reception, so it rewards reproducible work.
- **Paper archetypes.** Each paper is classified by its main contribution, such as a new
  method, a benchmark of existing ones, a survey, a dataset or a pipeline. Readers can tell
  what a paper offers before they open it.
- **OK-Graph.** A hierarchical map of a field: clusters of related work with generated titles
  and summaries, which can be expanded into the citation chains behind them, forwards or
  backwards in time from chosen seed papers.

## Principles

- **Transparency.** Discovery should not happen in a black box. Code, data sources and
  methods are open, and the platform favours open-access literature.
- **Comprehensiveness.** A top-down, hierarchical view gives a quick but complete picture
  of a field before going into detail.
- **Flexibility.** Researchers read literature in different ways, so the tool adapts to them:
  their own search terms, score weights and starting papers.

## Data

The platform works on the full [Semantic Scholar](https://www.semanticscholar.org/product/api)
corpus: about 255 million papers, their citation graph, abstracts and SPECTER2 embeddings.
[OpenAlex](https://openalex.org) fills in missing abstracts. Pipelines in this repository
archive every release in Google Cloud Storage, load it into ClickHouse and index papers into
OpenSearch, and follow Semantic Scholar's weekly updates. See the
[data pipeline documentation](docs/semantic-scholar.md).

## Repository layout

| Path | Contents |
| --- | --- |
| `packages/contracts` | Data models: datasets, identifiers, tables and the search document |
| `packages/core` | Domain logic, free of frameworks and clients |
| `packages/adapters` | Connections to storage, databases, search and data sources |
| `pipelines` | Prefect flows and the `okp` command line |
| `migrations/clickhouse` | Database schema migrations |
| `docs` | Detailed documentation |

Dependencies point one way: `pipelines` > `adapters` > `core` > `contracts`.
[AGENTS.md](AGENTS.md) lists the architecture rules that CI enforces.

## Development

Requires Python 3.12 and [uv](https://docs.astral.sh/uv/).

```sh
uv sync --all-packages --all-extras
uv run okp --help
```

Run the checks before opening a pull request; CI runs the same ones:

```sh
uv run pytest -q
uv run ruff check . && uv run ruff format --check .
uv run pyright
uv run lint-imports
```

End-to-end tests need Docker and are skipped without it.

## Documentation

- [Semantic Scholar archives](docs/semantic-scholar.md)
- [Loading into ClickHouse](docs/clickhouse.md)
- [SPECTER2 vector lookup](docs/embeddings.md)
- [Indexing papers into OpenSearch](docs/search-indexing.md)
- [Deployment](docs/deployment.md)

## License

[GNU Affero General Public License v3.0](LICENSE).
