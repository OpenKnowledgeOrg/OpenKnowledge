"""Candidate OpenAlex works for each live S2 paper, the rows of ``paper_xwalk``.

A DOI is normalised as the OpenAlex load normalises ``doi_norm``; an arXiv ID becomes its
``10.48550/arxiv.`` DOI. A title is lower-cased, each run of characters other than letters and
digits becomes one space, and an empty title never matches. A paper's candidates rank by key
(DOI, PubMed, then MAG with the same title and a year at most one apart), then works with an
abstract, non-XPAC works, non-preprints and the lower work ID; rank 1 is the chosen work.
"""

from openknowledge_adapters.clickhouse.parquet_sql import strip_doi

MATCH_KEY = "Enum8('doi' = 1, 'pmid' = 2, 'mag' = 3)"
XWALK_COLUMNS = "corpusid, work_id, match_key, rank, chosen, shared_work"


def doi_norm(column: str) -> str:
    stripped = strip_doi(column)
    return f"if({column} IS NULL, NULL, if(startsWith({stripped}, '10.'), {stripped}, NULL))"


def title_norm(column: str) -> str:
    return (
        f"trimBoth(replaceRegexpAll(lowerUTF8(ifNull({column}, '')), "
        "'[^\\\\p{L}\\\\p{N}]+', ' '))"
    )


def _s2(keys: str | None) -> str:
    arxiv = "trimBoth(ifNull(externalids['ArXiv'], ''))"
    within = f" AND corpusid IN ({keys})" if keys else ""
    return (
        f"SELECT corpusid, {doi_norm("externalids['DOI']")} AS doi, "
        f"if({arxiv} = '', NULL, concat('10.48550/arxiv.', "
        f"replaceRegexpOne(lower({arxiv}), 'v[0-9]+$', ''))) AS arxiv_doi, "
        f"{_number("externalids['PubMed']")} AS pmid, {_number("externalids['MAG']")} AS mag, "
        f"{title_norm('title')} AS title_norm, year "
        f"FROM papers FINAL WHERE is_deleted = 0{within}"
    )


def _number(column: str) -> str:
    return f"nullIf(toUInt64OrNull(trimBoth(ifNull({column}, ''))), 0)"


# A work whose index the abstracts load could not read has no abstract to offer.
_WORKS = (
    "SELECT work_id, doi_norm, pmid, mag, has_abstract AND work_id NOT IN ("
    "SELECT work_id FROM openalex_abstracts FINAL "
    "WHERE index_broken AND is_deleted = 0 AND NOT removed) AS with_abstract, is_xpac, "
    "ifNull(type, '') = 'preprint' AS is_preprint{extra} "
    "FROM openalex_works FINAL WHERE is_deleted = 0 AND NOT removed{where}"
)
_CANDIDATE = "w.work_id, toUInt8({order}), w.with_abstract, w.is_xpac, w.is_preprint"


def ranked_sql(keys: str | None) -> str:
    """Every candidate work of the papers in ``keys`` (all live papers without), ranked.

    The works side stays on the left, so the hash table holds the papers: few in a weekly
    update, spilled by ``grace_hash`` in a full build.
    """
    s2 = _s2(keys)
    works = _WORKS.format(extra="", where="")
    works_mag = _WORKS.format(
        extra=f", {title_norm('title')} AS title_norm, publication_year",
        where=" AND mag IS NOT NULL",
    )
    return (
        "SELECT corpusid, work_id, key_order, toUInt8(least(row_number() OVER ("
        "PARTITION BY corpusid "
        "ORDER BY key_order, with_abstract DESC, is_xpac, is_preprint, work_id), 255)) AS rank "
        "FROM (SELECT corpusid, work_id, min(key_order) AS key_order, "
        "any(with_abstract) AS with_abstract, any(is_xpac) AS is_xpac, "
        "any(is_preprint) AS is_preprint FROM ("
        "SELECT s.corpusid AS corpusid, w.work_id AS work_id, toUInt8(1) AS key_order, "
        "w.with_abstract AS with_abstract, w.is_xpac AS is_xpac, w.is_preprint AS is_preprint "
        f"FROM ({works}) AS w INNER JOIN ({s2}) AS s ON w.doi_norm = s.doi "
        f"UNION ALL SELECT s.corpusid, {_CANDIDATE.format(order=1)} "
        f"FROM ({works}) AS w INNER JOIN ({s2}) AS s ON w.doi_norm = s.arxiv_doi "
        f"UNION ALL SELECT s.corpusid, {_CANDIDATE.format(order=2)} "
        f"FROM ({works}) AS w INNER JOIN ({s2}) AS s ON w.pmid = s.pmid "
        f"UNION ALL SELECT s.corpusid, {_CANDIDATE.format(order=3)} "
        f"FROM ({works_mag}) AS w INNER JOIN ({s2}) AS s ON w.mag = s.mag "
        "WHERE s.title_norm != '' AND w.title_norm = s.title_norm "
        "AND abs(toInt32(s.year) - toInt32(w.publication_year)) <= 1"
        ") GROUP BY corpusid, work_id)"
    )


def xwalk_select(keys: str | None) -> str:
    """The ``paper_xwalk`` rows of the papers in ``keys``, or of every live paper.

    With ``keys``, it runs after their rows were deleted, so the other papers choosing the same
    work are counted from the table.
    """
    if keys is None:
        return (
            f"SELECT corpusid, work_id, CAST(key_order AS {MATCH_KEY}) AS match_key, rank, "
            "rank = 1 AS chosen, "
            "rank = 1 AND countIf(rank = 1) OVER (PARTITION BY work_id) > 1 AS shared_work "
            f"FROM ({ranked_sql(None)})"
        )
    ranked = ranked_sql(keys)
    return (
        f"SELECT c.corpusid, c.work_id, CAST(c.key_order AS {MATCH_KEY}) AS match_key, c.rank, "
        "c.rank = 1 AS chosen, "
        "c.rank = 1 AND countIf(c.rank = 1) OVER (PARTITION BY c.work_id) + o.others > 1 "
        f"AS shared_work FROM ({ranked}) AS c LEFT JOIN ("
        "SELECT work_id, count() AS others FROM paper_xwalk "
        f"WHERE chosen AND corpusid NOT IN ({keys}) "
        f"AND work_id IN (SELECT work_id FROM ({ranked}) WHERE rank = 1) "
        "GROUP BY work_id) AS o ON o.work_id = c.work_id"
    )


def expansion_sql(delta: str) -> str:
    """The papers outside ``delta`` that choose a work a delta paper chose or will choose.

    Their own choice cannot change, since their inputs did not, so one pass closes the set.
    """
    keys = f"SELECT key FROM {delta}"
    return (
        "SELECT DISTINCT corpusid FROM paper_xwalk "
        f"WHERE chosen AND corpusid NOT IN ({keys}) AND work_id IN ("
        f"SELECT work_id FROM paper_xwalk WHERE chosen AND corpusid IN ({keys}) "
        f"UNION DISTINCT SELECT work_id FROM ({ranked_sql(keys)}) WHERE rank = 1)"
    )


SOURCE = "Enum8('s2' = 1, 'openalex' = 2)"
BEST_COLUMNS = "corpusid, abstract, source, work_id"


def best_select(xwalk: str, keys: str | None) -> str:
    """One abstract per live paper in ``keys``, or per live paper without ``keys``.

    It is the live S2 abstract, even an empty one, else the non-empty abstract of the paper's
    chosen work in ``xwalk``. The large sides are joined rather than tested with IN, whose sets
    cannot spill to disk: the live papers alone would take about 6.5 GB.
    """
    within = f" AND corpusid IN ({keys})" if keys else ""
    works = f" AND work_id IN (SELECT work_id FROM {xwalk} WHERE chosen{within})" if keys else ""
    live_s2 = f"SELECT corpusid, abstract FROM abstracts FINAL WHERE is_deleted = 0{within}"
    return (
        "SELECT a.corpusid AS corpusid, a.abstract AS abstract, "
        f"CAST(1 AS {SOURCE}) AS source, CAST(NULL AS Nullable(UInt64)) AS work_id "
        f"FROM ({live_s2}) AS a INNER JOIN ("
        f"SELECT corpusid FROM papers FINAL WHERE is_deleted = 0{within}"
        ") AS p ON p.corpusid = a.corpusid "
        f"UNION ALL SELECT x.corpusid, o.abstract, CAST(2 AS {SOURCE}), x.work_id "
        "FROM (SELECT work_id, abstract FROM openalex_abstracts FINAL "
        f"WHERE is_deleted = 0 AND NOT removed AND abstract != ''{works}) AS o "
        f"INNER JOIN (SELECT corpusid, work_id FROM {xwalk} WHERE chosen{within}) AS x "
        "ON o.work_id = x.work_id "
        f"LEFT ANTI JOIN ({live_s2}) AS s ON s.corpusid = x.corpusid"
    )
