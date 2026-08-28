# Engineering Decisions & Trade-offs

This document covers the non-obvious design decisions in this pipeline, and
one real production bug that came up while building it — kept here instead
of smoothed over, because how a bug gets found and fixed is usually more
informative than a pipeline that never had one.

## Incident: mixed-case `region=` partitions in Bronze

**What happened:** the ingestion Lambda originally used a single `region`
variable, upper-cased once and reused for two different purposes — the
`regionCode` parameter sent to the YouTube Data API (which requires
uppercase, e.g. `IN`, `GB`), and the S3 partition key written to Bronze.
Athena/Glue partition keys are conventionally lowercase, but because the
same uppercase string was reused for both, S3 ended up with two separate
partitions per region — `region=GB/` and `region=gb/` — depending on
whichever casing happened to be in the `YOUTUBE_REGIONS` env var or left
over from an earlier run.

**Impact:** downstream Glue crawlers registered both partitions as distinct
values, so `SELECT DISTINCT region FROM raw_statistics` returned duplicates
like `GB` and `gb`, and any query filtering on `region = 'gb'` silently
missed rows sitting under `region=GB/`. Aggregations in the Gold layer were
undercounting for any region affected.

**Fix, in two parts:**
1. **Root cause** — `lambdas/ingestion/lambda_function.py` now keeps two
   explicitly named variables: `api_region` (uppercase, sent to the YouTube
   API) and `s3_region` (lowercase, used for every S3 key and partition).
   They're never conflated again.
2. **Remediation** — `scripts/migrate_region_partitions.py` is a one-time,
   idempotent migration script that scans a bucket for any non-lowercase
   `region=` prefix, copies the objects under the correct lowercase prefix,
   and deletes the originals. It supports `--dry-run` so you can see exactly
   what it would touch before it touches anything, and it's safe to re-run.

This is the kind of bug that's invisible until someone runs a `COUNT(*)`
that doesn't add up — worth documenting because the fix is really two fixes:
stop it from happening again, and clean up the mess it already made.

## Why Medallion (Bronze / Silver / Gold) instead of a single ETL step

Keeping raw API/CSV data untouched in Bronze means a bug in a transform can
be fixed and *replayed* against the original data — nothing is lost by a
bad Silver or Gold run. It also lets Bronze ingestion (Lambda, cheap, fast)
and Silver/Gold transforms (Glue/Spark, heavier) scale and fail independently.

## Why the DQ gate blocks Gold instead of just logging warnings

Early versions logged data quality issues but let the pipeline continue.
That meant a bad ingestion (e.g. an API quota error returning a near-empty
response) could silently produce a Gold `trending_analytics` row for that
day with a handful of records — the numbers would just look wrong, with no
signal to a downstream consumer that anything was off. The DQ Lambda now
runs as a hard gate between Silver and Gold: `quality_passed: false` stops
the Step Functions execution before Gold aggregation runs, and fires an SNS
alert. Silent bad data was worse than a loud pipeline failure.

## Why both Kaggle CSV and live API JSON are supported in one Glue job

The original project prototype was built against the static Kaggle
"YouTube New" dataset. Rather than throw that schema-handling logic away
once live API ingestion was added, `bronze_to_silver_statistics.py` detects
which format it's reading (by checking for `snippet.title` vs the Kaggle
column names) and branches accordingly. This means historical Kaggle data
and live API data can be backfilled/queried through the exact same Silver
table and downstream Gold aggregations — one schema, one set of consumers,
two ingestion paths.

## Known limitations / what I'd do with more time

- **No automated tests yet.** CI currently runs linting (`flake8`, `black`)
  and JSON validation on the Step Functions definition and IAM policies.
  The next step would be `pytest` unit tests for the DQ check functions and
  the region-flattening logic in the Glue jobs (both are pure functions of
  a DataFrame, so they're testable without a live Glue/Spark cluster using
  `pyspark.sql` local mode or mocked DataFrames).
- **IAM policies are broad in places** (e.g. `Resource: "*"` on some Glue
  actions) — fine for a personal/dev AWS account, but I'd scope these down
  to specific database/table ARNs before using this pattern in a shared
  or production AWS account.
- **No infrastructure-as-code yet.** Buckets, Glue jobs, and the state
  machine are currently created via the AWS CLI commands in the README.
  Terraform or AWS CDK would make this reproducible and reviewable instead
  of a list of commands to run in order.
