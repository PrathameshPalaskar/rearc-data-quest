

# PROCESS.md

## Architecture

**Catalog layout.** Everything lives under a single Unity Catalog catalog, `rearc_bls`,
split into three schemas that map directly to the medallion layers: `bronze`, `silver`,
and `gold`. I chose separate schemas per layer rather than one flat schema with
prefixed table names because it makes access control a first-class concern instead of
an afterthought — a read-only analyst role can be granted `USE SCHEMA` + `SELECT` on
`rearc_bls.gold` alone, with `bronze` and `silver` staying invisible to them.

The whole pipeline is built as a single **Lakeflow Spark Declarative Pipeline** (the
current name for what was previously called Delta Live Tables), using the
`pyspark.pipelines` (`dp`) API — `@dp.table` for tables and `@dp.materialized_view`
for the Gold aggregation outputs, rather than the legacy `dlt` import.

**Bronze.** Bronze tables are near-verbatim loads of everything landed in the
`rearc_bls.bronze.raw_landing` Volume:
- `bronze_bls_pr` — the core BLS productivity series values (`pr.data.0.Current`)
- `bronze_bls_series` — series metadata (codes only, BLS does not ship a plain-English
  title column)
- `bronze_bls_sector`, `bronze_bls_class`, `bronze_bls_measure`, `bronze_bls_duration`
  — BLS's own code lookup files, needed to decode a `series_id` into something a
  human can read
- `bronze_population` — the DataUSA population API response, exploded from its
  top-level `data` array into one row per year

Bronze applies only light typing and one DLT expectation
(`series_id IS NOT NULL`) — 
This does not check data quality just to ensure we did not receive invalid data.

**Silver.** Silver deduplicates on each table's natural key (`series_id, year, period`
for BLS series values, `series_id` for series metadata, `year` for population) and
enforces stronger expectations (`year BETWEEN 1900 AND 2100`, `value IS NOT NULL`).
The code lookup tables pass through largely unchanged since they're small, static
reference data.

**Gold.** Three Gold outputs answer the three required questions, each implemented
once in SQL and once in PySpark:

- **Q1 (population mean/stddev, 2013–2018): SQL primary.** A single-table
  aggregation reads more clearly as a declarative `SELECT ... WHERE year BETWEEN`
  than as a PySpark call chain, and a materialized view gives "recompute when Silver
  changes" semantics for free.
- **Q2 (best year per series, with a human-readable label): PySpark primary.**
  This one needs a `Window` function to rank years within each `series_id`, which I
  find easier to read and modify as an explicit
  `Window.partitionBy(...).orderBy(...)` object than as a `ROW_NUMBER() OVER (...)`
  clause nested inside a CTE. The label itself is built by joining the series against
  BLS's own `sector`/`class`/`measure`/`duration` code lookup tables and concatenating
  their text fields — BLS doesn't provide a single "title" field, so the label has to
  be assembled from these component codes.
- **Q3 (PRS30006032, period Q01, joined with population): SQL primary.** A simple
  filter + left join, clearest as SQL.

Each Gold table's non-primary version is kept in the repo alongside the primary, to
show equal fluency in both rather than picking one language and sticking to it
throughout.

**Re-running ingestion safely.** The ingestion notebook re-lists the BLS directory on
every run (rather than hardcoding filenames), so additions/removals on BLS's side are
picked up automatically. Idempotency is handled by comparing a `HEAD` request's
`Content-Length` against the locally landed file's size before re-downloading —
unchanged files are skipped. Silver's `dropDuplicates` on each table's natural key is
a second line of defense against any duplicate landing. Gold materialized views
recompute deterministically from Silver, so re-running the whole pipeline is always
safe.

## Trade-offs

Decisions I'd revisit for a real client engagement, roughly in order of what I'd
tackle first:

- **Ingestion mechanism.** This `requests` + `BeautifulSoup` directory-listing
  scraper works for this exercise, but in production I'd use **Auto Loader**
  (`cloudFiles`) once files land in the Volume, so new-file discovery and incremental
  processing is handled natively rather than hand-rolled.
- **Schema drift.** Both BLS and the population API turned out to have real drift risk
  in practice (see Retrospective) — BLS pads headers with whitespace, and the
  population API's `Nation ID` field has a space that Delta rejects outright as a
  column name. For a client I'd add schema validation as an explicit, alerting step
  rather than something that only surfaces as a pipeline failure after the fact.
- **Data volume.** Both sources are small here. At real scale I'd partition
  Bronze/Silver by `year` and reconsider whether `dropDuplicates` full-table scans are
  still cheap enough, or whether a keyed `MERGE INTO` is needed instead.
- **Access control.** I implemented a minimal read-only grant on the `gold` schema for
  an "analysts" group. In practice I'd separate the pipeline's own service-principal
  write access from any human's read access, and consider column-level security if any
  series carried sensitive classifications.
- **Rate limiting / retries.** My ingestion script does a single `requests.get` per
  file with no retry/backoff. BLS does rate-limit; production ingestion needs
  exponential backoff on 429/5xx rather than letting the job fail outright.

## AI usage

I used Claude throughout as a drafting and debugging aid, not as a black box —
I validated or corrected its output at every step

- **Drafting the pipeline scaffold.** Claude drafted the initial Bronze/Silver/Gold
  structure and the SQL/PySpark pairs for the three Gold questions. I reviewed the
  window-function logic in Q2 and the join conditions in Q3 myself against BLS's own
  documentation rather than trusting the first draft.
- **Debugging a URL-joining bug.** My BLS ingestion script initially 404'd because it
  naively concatenated the base URL with an href that was already an absolute path
  (BLS's directory listing returns hrefs like `/pub/time.series/pr/pr.class`, not bare
  filenames). Claude diagnosed the double-path issue and suggested `urllib.parse.urljoin`;
  I verified the fix myself by printing the raw hrefs before and after.
- **API research.** Claude (with web search) confirmed DataUSA's population API had
  moved from a legacy endpoint to the current `api.datausa.io/tesseract/...` shape,
  and flagged that responses wrap records in a top-level `data` array requiring an
  `explode`. I cross-checked this against DataUSA's own docs rather than trusting it
  outright.
- **Debugging schema/column errors.** Several real errors came up that Claude helped
  root-cause but that I confirmed against the actual data myself before applying a
  fix, rather than accepting a guess: BLS's flat-file headers are padded with
  whitespace (`series_id        `) causing `UNRESOLVED_COLUMN` errors; `pr.series`
  does not have a `series_title` column as I'd initially assumed — I ran a direct
  schema inspection (`spark.read...columns`) myself and shared the real output before
  Claude wrote the decode-join logic against actual column names
  (`sector_name`, `class_text`, `measure_text`, `duration_text`); and DataUSA's API
  returns a field named `Nation ID` with a space, which Delta rejects as invalid,
  requiring a column-name sanitization step.
- **Duplicate table definition bug.** I hit a `DUPLICATE_OBJECT` error because
  `silver_bls_pr` had been copy-pasted into `gold.py` as well as `silver.py`. Claude
  helped narrow this down to a pipeline source-file issue and I confirmed by
  inspecting both files directly.
- **What I did NOT take as-is:** every schema/column assumption in this project was
  verified against the actual raw data (via `printSchema()`/`show()`) before being
  hardcoded into pipeline code — this was a repeated lesson across the build, not a
  one-off. The final decode join for human-readable series labels uses column names I
  confirmed directly from BLS's lookup files, not names guessed from convention.

## Retrospective
I could not solve this problem of human label.
The hardest part was **getting from `series_id` to a human-readable label**. BLS
doesn't ship a single "title" field — I initially assumed one existed
(`series_title`) and had to correct that assumption against the real schema. The
actual solution required landing four separate BLS code-lookup files
(`pr.sector`, `pr.class`, `pr.measure`, `pr.duration`) as their own Bronze/Silver
tables and joining all of them against `pr.series` to reconstruct something readable,
But in spite of multiple approaches it did not work as expected. 


A close second was **whitespace and invalid characters hiding in "clean-looking"
source data** — BLS pads its flat-file column headers with trailing spaces, and the
population API returns a column literally named `Nation ID`. Neither was visible
until Spark actually threw an `UNRESOLVED_COLUMN` or `DELTA_INVALID_CHARACTERS`
error; both needed a normalization step (`.toDF(*[c.strip() for c in raw.columns])`,
and a space-to-underscore replacement) applied right at the Bronze read, before
anything downstream could reference the columns by name.