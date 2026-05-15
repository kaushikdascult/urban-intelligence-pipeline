# ADR-001: Platform and tooling choices

**Status**: Accepted
**Date**: 2026-05-09
**Decision-maker**: Kaushik Das

## Context

Building a portfolio data engineering project to demonstrate production-grade GCP skills. Constraints: $300 free-trial budget, 6-day timeline, real org-policy restrictions (cannot create service account keys), Windows local development environment.

## Decisions

### 1. GCP over AWS / Azure

**Chosen:** Google Cloud Platform.

**Reasoning:** BigQuery's serverless model means no infrastructure to size or manage. The free public datasets (NYC TLC taxi data) live natively in BQ — no S3/blob copies needed. Dataproc Serverless gives me Spark without paying for idle clusters. The free trial offers $300 credits over 90 days, which is more than enough.

**Trade-off:** Tighter vendor lock-in vs Azure/AWS. Acceptable for a portfolio demo.

### 2. Dataproc Serverless over Dataproc cluster

**Chosen:** Dataproc Serverless (batch submission model).

**Reasoning:** Traditional Dataproc clusters bill per minute even when idle. For a pipeline that runs for ~5 minutes daily, that's wasteful. Serverless scales to zero and only bills during execution (~$0.10 per batch).

**Trade-off:** ~30 second cold start each run. Acceptable for batch workloads.

### 3. dbt-bigquery over custom SQL scripts

**Chosen:** dbt-bigquery 1.8.

**Reasoning:** dbt provides version control, dependency management (DAG), automated testing, lineage docs, and incremental models — for free. Compared to managing dozens of SQL scripts manually, dbt is industry-standard for the analytics-engineering layer.

**Trade-off:** Adds a tool to the stack. Worth it.

### 4. OAuth + ADC over service account keys

**Chosen:** Application Default Credentials with OAuth user delegation.

**Reasoning:** GCP best practice (and this org's enforced policy `constraints/iam.disableServiceAccountKeyCreation`) prohibits SA key creation. ADC handles auth automatically without exposing static credentials. For Airflow running in Docker, the host's `~/.config/gcloud/application_default_credentials.json` is mounted read-only into the container.

**Trade-off:** Requires user-level interactive login on first auth. Acceptable for portfolio; production would use Workload Identity Federation.

### 5. Docker Airflow over Cloud Composer (for development)

**Chosen:** Local Docker Compose with `apache/airflow:2.10.3-python3.11`.

**Reasoning:** Cloud Composer costs ~$70/month even when idle. For iterative development, a local Docker stack is free, fast, and produces nearly identical DAGs. The same DAG file works in Composer with minimal changes (use BashOperator + gsutil to fetch scripts from GCS at runtime instead of volume mounts).

**Trade-off:** Doesn't fully prove Composer skills. A 1-week Composer deployment is planned for the last 2 weeks of the trial to demonstrate cloud-managed orchestration.

### 6. Indirect write mode for the BigQuery Spark connector

**Chosen:** `writeMethod=indirect` + `createDisposition=CREATE_IF_NEEDED` when writing PySpark output to BQ.

**Reasoning:** Direct write mode silently drops partitioning configuration, producing unpartitioned tables. Discovered this when verifying via `INFORMATION_SCHEMA.PARTITIONS` and seeing single-NULL-partition output. Indirect mode (writes to GCS first, then loads to BQ) preserves all partition/cluster settings.

**Trade-off:** Slower than direct (extra GCS hop). Worth it for correctness.

### 7. Partition by `pickup_date`, cluster by `(pickup_hour, pickup_location_id)`

**Chosen:** Daily date partitioning + 2-column clustering on `fct_trips`.

**Reasoning:** Most analytical queries filter by date range and aggregate by hour-of-day or pickup zone. Partitioning prunes large numbers of rows; clustering co-locates rows in the same hour and zone, dramatically reducing bytes scanned for grouped queries.

**Trade-off:** Slightly higher write cost. Recovered many times over by query-time savings.

## Consequences

- Pipeline runs end-to-end for ~$0.12/run.
- Total spend across development: ~$1.50 of $300 budget.
- Architecture extends cleanly to Composer (one-DAG-file change) and additional sources (just add a new ingestion script + DAG task).
- Auth model works in any GCP org regardless of SA-key policy.

## References

- [GCP free trial terms](https://cloud.google.com/free)
- [Dataproc Serverless pricing](https://cloud.google.com/dataproc-serverless/pricing)
- [BigQuery Spark connector — write modes](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark)
- [Workload Identity Federation](https://cloud.google.com/iam/docs/workload-identity-federation) (production alternative to ADC)
