"""Candidate OpenAlex works for each live S2 paper, the rows of ``paper_xwalk``.

A DOI is normalised as the OpenAlex load normalises ``doi_norm``; an arXiv ID becomes its
``10.48550/arxiv.`` DOI. A title is lower-cased, each run of characters other than letters and
digits becomes one space, and an empty title never matches. A paper's candidates rank by key
(DOI, PubMed, then MAG with the same title and a year at most one apart), then works with an
abstract, non-XPAC works, non-preprints and the lower work ID; rank 1 is the chosen work.

A full build then matches papers with no ID candidate by exact title (``title_sql``).
"""

from openknowledge_adapters.clickhouse.parquet_sql import strip_doi

# 4 is kept for the relaxed MAG tier, so ID keys still sort before title keys.
MATCH_KEY = "Enum8('doi' = 1, 'pmid' = 2, 'mag' = 3, 'title' = 5)"
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

    Their own choice cannot change, since their inputs did not, so one pass closes the set. A
    paper that chose its work by title (``by_title``) loses it: title matches take free works
    only, and an update computes no title matches.
    """
    keys = f"SELECT key FROM {delta}"
    return (
        "SELECT DISTINCT corpusid, match_key = 'title' AS by_title FROM paper_xwalk "
        f"WHERE chosen AND corpusid NOT IN ({keys}) AND work_id IN ("
        f"SELECT work_id FROM paper_xwalk WHERE chosen AND corpusid IN ({keys}) "
        f"UNION DISTINCT SELECT work_id FROM ({ranked_sql(keys)}) WHERE rank = 1)"
    )


TITLE_PAPERS = "read_title_papers"
TITLE_WORKS = "read_title_works"
TITLE_CANDIDATES = "read_title_candidates"
TITLE_SCRATCH = (TITLE_PAPERS, TITLE_WORKS, TITLE_CANDIDATES)
"""Scratch tables of a full build's title matching."""
MIN_TITLE_CHARS = 20
"""Shorter titles with a year matched below 97% precision in the 1% sample."""
MIN_TITLE_CHARS_NO_YEAR = 40
MAX_TITLE_WORKS = 1000
"""A title on more live works than this is generic."""
SERIAL_YEARS = 3
"""A title whose eligible works span this many publication years is a recurring serial."""
EXCLUDED_TYPES = ("dataset", "peer-review", "erratum", "retraction")
REPOSITORY_DOIS = ("10.17632/", "10.6084/", "10.5281/")
"""Mendeley Data, Figshare and Zenodo deposits, which often copy an article's title."""
_LONG = f"chars >= if(year IS NULL, {MIN_TITLE_CHARS_NO_YEAR}, {MIN_TITLE_CHARS})"
_NEAR = "abs(toInt32({a}) - toInt32({b})) <= 1"


def title_sql(xwalk: str, settings: str) -> list[str]:
    """Statements adding exact-title matches to ``xwalk``, which holds every ID candidate.

    Only papers without any candidate are matched, each to at most one work (rank 1), which no
    paper chose yet. A paper matches a work of equal normalised title (partitioned by its 64-bit
    hash and verified by string equality) and a year at most one apart; a paper without a year
    matches across all years with a longer title. Its window must hold exactly one eligible work,
    a non-XPAC one, or else one XPAC work; the work's window must hold no other live paper of
    that title, matched or not. Generic and serial titles, data deposits, paratext and a few
    types never match, and a work of no clear type needs the same year.

    Every large lookup is expressed as a join rather than an ``IN`` set (which cannot spill in
    ClickHouse 26.8), so ``settings`` should use ``grace_hash``.
    """
    eligible = " AND ".join(
        [
            "NOT is_paratext",
            f"ifNull(type, '') NOT IN ({', '.join(f"'{kind}'" for kind in EXCLUDED_TYPES)})",
            *(f"NOT startsWith(ifNull(doi_norm, ''), '{prefix}')" for prefix in REPOSITORY_DOIS),
        ]
    )
    titled = f"{title_norm('title')} AS title_norm"
    kept_titles = (
        f"SELECT th, title_norm FROM {TITLE_WORKS} GROUP BY th, title_norm "
        f"HAVING count() <= {MAX_TITLE_WORKS} AND uniqExactIf(year, eligible) < {SERIAL_YEARS}"
    )
    pairs = (
        "SELECT p.corpusid AS corpusid, p.th AS th, p.title_norm AS title_norm, p.year AS year, "
        "w.work_id AS work_id, w.year AS work_year, w.is_xpac AS is_xpac, w.unclear AS unclear "
        "FROM ("
        "SELECT w.th AS th, w.title_norm AS title_norm, w.work_id AS work_id, "
        "w.year AS year, w.is_xpac AS is_xpac, w.unclear AS unclear "
        f"FROM {TITLE_WORKS} AS w "
        f"INNER JOIN ({kept_titles}) AS k ON k.th = w.th AND k.title_norm = w.title_norm "
        "WHERE w.eligible) AS w "
        "INNER JOIN ("
        f"SELECT corpusid, th, title_norm, year FROM {TITLE_PAPERS} WHERE unmatched AND {_LONG}"
        ") AS p ON p.th = w.th AND p.title_norm = w.title_norm "
        f"WHERE p.year IS NULL OR (w.year IS NOT NULL AND {_NEAR.format(a='p.year', b='w.year')})"
    )
    # The candidate is the one non-XPAC work, else the only work.
    picked = (
        "SELECT corpusid, any(th) AS th, any(title_norm) AS title_norm, any(year) AS year, "
        "argMax(tuple(work_id, work_year, unclear), NOT is_xpac) AS pick "
        f"FROM ({pairs}) GROUP BY corpusid "
        "HAVING countIf(NOT is_xpac) = 1 OR count() = 1"
    )
    alone = (
        f"SELECT c.corpusid AS corpusid FROM {TITLE_PAPERS} AS s "
        f"INNER JOIN {TITLE_CANDIDATES} AS c ON s.th = c.th AND s.title_norm = c.title_norm "
        "WHERE c.year IS NULL OR c.work_year IS NULL OR s.year IS NULL "
        f"OR {_NEAR.format(a='s.year', b='c.work_year')} "
        "GROUP BY c.corpusid HAVING count() = 1"
    )
    taken = (
        f"SELECT DISTINCT x.work_id AS work_id FROM {xwalk} AS x "
        f"INNER JOIN (SELECT DISTINCT work_id FROM {TITLE_CANDIDATES}) AS c "
        "ON c.work_id = x.work_id WHERE x.chosen"
    )
    return [
        *(f"DROP TABLE IF EXISTS {table}" for table in TITLE_SCRATCH),
        f"CREATE TABLE {TITLE_PAPERS} (th UInt64, title_norm String, corpusid UInt64, "
        "year Nullable(Int32), chars UInt32, unmatched Bool) ENGINE = MergeTree ORDER BY th",
        f"INSERT INTO {TITLE_PAPERS} (th, title_norm, corpusid, year, chars, unmatched) "
        "SELECT cityHash64(p.title_norm), p.title_norm, p.corpusid, p.year, "
        "lengthUTF8(p.title_norm), NOT ifNull(m.matched, false) "
        f"FROM (SELECT corpusid, {titled}, year FROM papers FINAL "
        "WHERE is_deleted = 0 AND title_norm != '') AS p "
        f"LEFT JOIN (SELECT corpusid, true AS matched FROM {xwalk} WHERE rank = 1) AS m "
        f"ON m.corpusid = p.corpusid {settings}",
        f"CREATE TABLE {TITLE_WORKS} (th UInt64, title_norm String, work_id UInt64, "
        "year Nullable(Int32), is_xpac Bool, eligible Bool, unclear Bool) "
        "ENGINE = MergeTree ORDER BY th",
        f"INSERT INTO {TITLE_WORKS} (th, title_norm, work_id, year, is_xpac, eligible, unclear) "
        "SELECT w.th, w.title_norm, w.work_id, w.publication_year, w.is_xpac, "
        "w.eligible, w.unclear FROM ("
        "SELECT cityHash64(title_norm) AS th, title_norm, work_id, publication_year, is_xpac, "
        f"{eligible} AS eligible, ifNull(type, '') IN ('', 'other') AS unclear "
        "FROM (SELECT work_id, publication_year, is_xpac, is_paratext, type, doi_norm, "
        f"{titled} FROM openalex_works FINAL "
        "WHERE is_deleted = 0 AND NOT removed AND title_norm != '')) AS w "
        "INNER JOIN ("
        f"SELECT DISTINCT th, title_norm FROM {TITLE_PAPERS} WHERE unmatched AND {_LONG}"
        f") AS u ON u.th = w.th AND u.title_norm = w.title_norm {settings}",
        f"CREATE TABLE {TITLE_CANDIDATES} (corpusid UInt64, th UInt64, title_norm String, "
        "year Nullable(Int32), work_id UInt64, work_year Nullable(Int32)) "
        "ENGINE = MergeTree ORDER BY corpusid",
        f"INSERT INTO {TITLE_CANDIDATES} (corpusid, th, title_norm, year, work_id, work_year) "
        f"SELECT corpusid, th, title_norm, year, pick.1, pick.2 FROM ({picked}) "
        f"WHERE NOT pick.3 OR pick.2 = year {settings}",
        f"INSERT INTO {xwalk} ({XWALK_COLUMNS}) "
        f"SELECT c.corpusid, c.work_id, CAST('title' AS {MATCH_KEY}), toUInt8(1), true, false "
        f"FROM {TITLE_CANDIDATES} AS c "
        f"INNER JOIN ({alone}) AS a ON a.corpusid = c.corpusid "
        f"LEFT ANTI JOIN ({taken}) AS t ON t.work_id = c.work_id {settings}",
    ]


def title_stats_sql(xwalk: str) -> str:
    """Candidates of the title step, those whose work an ID match took, and the matches."""
    return (
        "SELECT count() AS candidates, countIf(t.is_taken = 1) AS taken, "
        f"(SELECT count() FROM {xwalk} WHERE match_key = 'title') AS matched "
        f"FROM {TITLE_CANDIDATES} AS c LEFT JOIN ("
        f"SELECT DISTINCT x.work_id AS work_id, toUInt8(1) AS is_taken FROM {xwalk} AS x "
        f"INNER JOIN (SELECT DISTINCT work_id FROM {TITLE_CANDIDATES}) AS k "
        "ON k.work_id = x.work_id WHERE x.chosen AND x.match_key != 'title'"
        ") AS t ON t.work_id = c.work_id"
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
