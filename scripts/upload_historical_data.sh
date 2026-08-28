#!/usr/bin/env bash
#
# Upload historical Kaggle YouTube Trending data to the Bronze S3 layer.
#
# This is an optional backfill step — the pipeline's primary data source is
# the live YouTube Data API v3 (see lambdas/ingestion/). This script exists
# to seed Bronze with historical data for backtesting/demoing the Silver and
# Gold transforms against a larger volume than a few days of live API pulls.
#
# Dataset: https://www.kaggle.com/datasets/datasnaek/youtube-new
# Download the CSVs + *_category_id.json files for whichever regions you
# want, place them in a local ./data directory, then run this script.
#
# Usage:
#   export BRONZE_BUCKET=yt-pipeline-bronze-ap-south-1-dev
#   ./upload_historical_data.sh ./data

set -euo pipefail

DATA_DIR="${1:-./data}"
BUCKET="${BRONZE_BUCKET:?Set BRONZE_BUCKET env var, e.g. export BRONZE_BUCKET=yt-pipeline-bronze-ap-south-1-dev}"
REGIONS=(ca de fr gb in jp kr mx ru us)

if [ ! -d "$DATA_DIR" ]; then
  echo "ERROR: data directory '$DATA_DIR' not found." >&2
  exit 1
fi

for region in "${REGIONS[@]}"; do
  upper_region=$(echo "$region" | tr '[:lower:]' '[:upper:]')
  csv_file="$DATA_DIR/${upper_region}videos.csv"
  json_file="$DATA_DIR/${upper_region}_category_id.json"

  if [ -f "$csv_file" ]; then
    aws s3 cp "$csv_file" "s3://${BUCKET}/youtube/raw_statistics/region=${region}/"
  else
    echo "  skip: $csv_file not found"
  fi

  if [ -f "$json_file" ]; then
    aws s3 cp "$json_file" "s3://${BUCKET}/youtube/raw_statistics_reference_data/region=${region}/"
  else
    echo "  skip: $json_file not found"
  fi
done

echo "Done. Verify with:"
echo "  aws s3 ls s3://${BUCKET}/youtube/raw_statistics/ --recursive"
