#!/usr/bin/env bash
#
# transform_backfill.sh — backfill staging.taxi_trips_enriched one partition
# at a time by submitting Dataproc Serverless batches SYNCHRONOUSLY.
#
# Why synchronous (no --async):
#   The first attempt at this backfill used `gcloud ... --async` in a loop.
#   All 31 submits landed in ~1 minute, but Dataproc Serverless on a new
#   project has a low concurrent-batch quota — only 2 got slots, the other
#   29 were rejected at submission with RESOURCE_EXHAUSTED. Because --async
#   returns immediately, the loop had already finished; freed slots opened
#   to an empty queue and nothing retried.
#
#   Submitting synchronously makes `gcloud` block until each batch reaches a
#   terminal state. The next submit only happens once a slot is free, so the
#   quota ceiling becomes a natural pacing mechanism instead of a wall.
#
# Behaviour:
#   - Resumable: --start-date / --end-date define an inclusive range.
#   - Idempotent: spark_transform.py does DELETE-then-APPEND per partition,
#     so re-running a date that already succeeded is safe (no duplicates).
#   - Skip-and-report: a FAILED batch is logged and the loop continues;
#     the script exits non-zero at the end if anything failed, and prints
#     the exact list of dates to re-run.
#   - Verify-before-run: checks gcloud auth + Dataproc reachability first.
#
# Usage:
#   ./scripts/transform_backfill.sh --start-date 2022-01-01 --end-date 2022-01-31
#   ./scripts/transform_backfill.sh --start-date 2022-01-02 --end-date 2022-01-28
#
# Re-running only the days that failed: just pass a narrower range, or run
# the full range again — succeeded days are harmless to repeat.

set -uo pipefail   # NOT -e: we handle batch failures explicitly, per-day.

# -----------------------------------------------------------------------------
# Config — override via environment if needed
# -----------------------------------------------------------------------------
PROJECT_ID="${PROJECT_ID:-urban-pipeline-kd-2026}"
REGION="${REGION:-us-central1}"
SA="${SA:-urban-pipeline-sa@urban-pipeline-kd-2026.iam.gserviceaccount.com}"
SCRIPT_URI="${SCRIPT_URI:-gs://urban-pipeline-kd-2026-scripts/transform/spark_transform.py}"
RUNTIME_VERSION="${RUNTIME_VERSION:-2.2}"
BQ_JAR="${BQ_JAR:-gs://spark-lib/bigquery/spark-3.5-bigquery-0.42.0.jar}"
RAW_DATASET="${RAW_DATASET:-raw}"
STAGING_DATASET="${STAGING_DATASET:-staging}"
GCS_TEMP_BUCKET="${GCS_TEMP_BUCKET:-urban-pipeline-kd-2026-staging}"

# -----------------------------------------------------------------------------
# Arg parsing
# -----------------------------------------------------------------------------
START_DATE=""
END_DATE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --start-date) START_DATE="$2"; shift 2 ;;
    --end-date)   END_DATE="$2";   shift 2 ;;
    -h|--help)
      grep '^#' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "ERROR: unknown argument: $1" >&2
      echo "Usage: $0 --start-date YYYY-MM-DD --end-date YYYY-MM-DD" >&2
      exit 2
      ;;
  esac
done

if [[ -z "$START_DATE" || -z "$END_DATE" ]]; then
  echo "ERROR: both --start-date and --end-date are required." >&2
  echo "Usage: $0 --start-date YYYY-MM-DD --end-date YYYY-MM-DD" >&2
  exit 2
fi

# Validate date format (YYYY-MM-DD) and that they parse as real dates.
date_to_epoch() {
  # GNU date (Termux/Linux): date -d. BSD date (macOS) would need -j -f.
  date -u -d "$1" +%s 2>/dev/null
}

START_EPOCH="$(date_to_epoch "$START_DATE")"
END_EPOCH="$(date_to_epoch "$END_DATE")"

if [[ -z "$START_EPOCH" || -z "$END_EPOCH" ]]; then
  echo "ERROR: could not parse dates. Expected YYYY-MM-DD." >&2
  echo "  --start-date='$START_DATE'  --end-date='$END_DATE'" >&2
  exit 2
fi

if (( START_EPOCH > END_EPOCH )); then
  echo "ERROR: --start-date must be on or before --end-date." >&2
  exit 2
fi

# -----------------------------------------------------------------------------
# Pre-flight: confirm gcloud is authenticated and Dataproc is reachable
# -----------------------------------------------------------------------------
echo "============================================================"
echo "Pre-flight checks"
echo "============================================================"

if ! command -v gcloud >/dev/null 2>&1; then
  echo "ERROR: 'gcloud' not found on PATH." >&2
  echo "Install the Google Cloud CLI in this shell before running." >&2
  exit 3
fi

ACTIVE_ACCT="$(gcloud auth list --filter=status:ACTIVE --format='value(account)' 2>/dev/null)"
if [[ -z "$ACTIVE_ACCT" ]]; then
  echo "ERROR: no active gcloud credentials." >&2
  echo "Run: gcloud auth login   (and: gcloud auth application-default login)" >&2
  exit 3
fi
echo "Active account: $ACTIVE_ACCT"

# Cheap reachability probe — lists batches (also warms up auth).
if ! gcloud dataproc batches list --region="$REGION" --project="$PROJECT_ID" \
       --limit=1 --format='value(name)' >/dev/null 2>&1; then
  echo "ERROR: cannot reach Dataproc in $REGION for project $PROJECT_ID." >&2
  echo "Check network connectivity and that the Dataproc API is enabled." >&2
  exit 3
fi
echo "Dataproc reachable in $REGION."
echo "Project: $PROJECT_ID"
echo "Script:  $SCRIPT_URI"
echo "Range:   $START_DATE -> $END_DATE (inclusive)"
echo

# -----------------------------------------------------------------------------
# Main loop
# -----------------------------------------------------------------------------
SUCCEEDED=()
FAILED=()

CUR_EPOCH="$START_EPOCH"
DAY_SECONDS=86400

while (( CUR_EPOCH <= END_EPOCH )); do
  ING_DATE="$(date -u -d "@${CUR_EPOCH}" +%Y-%m-%d)"
  # Batch IDs must be unique and lowercase; date + timestamp keeps them so.
  BATCH_ID="urban-pipeline-bf-${ING_DATE}-$(date +%s)"

  echo "------------------------------------------------------------"
  echo "=== $ING_DATE  (batch: $BATCH_ID) ==="
  echo "------------------------------------------------------------"

  # Synchronous submit: gcloud blocks until the batch reaches a terminal
  # state. Exit code is non-zero if the batch FAILED, CANCELLED, or the
  # submit itself was rejected (e.g. RESOURCE_EXHAUSTED).
  if gcloud dataproc batches submit pyspark \
      "$SCRIPT_URI" \
      --batch="$BATCH_ID" \
      --region="$REGION" \
      --project="$PROJECT_ID" \
      --service-account="$SA" \
      --version="$RUNTIME_VERSION" \
      --jars="$BQ_JAR" \
      -- \
      --project="$PROJECT_ID" \
      --raw-dataset="$RAW_DATASET" \
      --staging-dataset="$STAGING_DATASET" \
      --gcs-temp-bucket="$GCS_TEMP_BUCKET" \
      --ingestion-date="$ING_DATE"
  then
    echo ">>> $ING_DATE SUCCEEDED"
    SUCCEEDED+=("$ING_DATE")
  else
    RC=$?
    echo ">>> $ING_DATE FAILED (exit code $RC) — continuing" >&2
    FAILED+=("$ING_DATE")
  fi

  CUR_EPOCH=$(( CUR_EPOCH + DAY_SECONDS ))
done

# -----------------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------------
echo
echo "============================================================"
echo "Backfill complete"
echo "  Succeeded: ${#SUCCEEDED[@]}"
echo "  Failed:    ${#FAILED[@]}"
echo "============================================================"

if (( ${#FAILED[@]} > 0 )); then
  echo "Failed dates:" >&2
  for d in "${FAILED[@]}"; do
    echo "  $d" >&2
  done
  echo >&2
  # Emit a ready-to-paste re-run hint for contiguous or scattered failures.
  echo "Re-run them by passing a range that covers the failed dates, e.g.:" >&2
  echo "  $0 --start-date ${FAILED[0]} --end-date ${FAILED[-1]}" >&2
  echo "(succeeded days in that range are idempotent — safe to repeat)" >&2
  exit 1
fi

echo "All ${#SUCCEEDED[@]} partitions written successfully."
exit 0
