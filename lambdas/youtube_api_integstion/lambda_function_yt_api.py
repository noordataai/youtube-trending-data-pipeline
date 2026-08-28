"""
Lambda: YouTube Data API Ingestion (Bronze Layer)
──────────────────────────────────────────────────
Triggered by EventBridge on a schedule (e.g., every 6 hours).
Pulls trending videos from the YouTube Data API for each configured region
and writes raw JSON responses to the Bronze S3 bucket.

This replaces the old "download from Kaggle and aws s3 cp" workflow
with a real, automated, live data ingestion pipeline.

Environment Variables:
    YOUTUBE_API_KEY       — Google API key with YouTube Data API v3 enabled
    S3_BUCKET_BRONZE      — Target S3 bucket for raw data
    YOUTUBE_REGIONS       — Comma-separated region codes (default: US,GB,CA,...)
    SNS_ALERT_TOPIC_ARN   — SNS topic for failure alerts
"""

import json
import os
import logging
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode

import boto3

# ── Logging ──────────────────────────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── AWS Clients ──────────────────────────────────────────────────────────────
s3_client = boto3.client("s3")
sns_client = boto3.client("sns")

# ── Config ───────────────────────────────────────────────────────────────────
API_KEY = os.environ["YOUTUBE_API_KEY"]
BUCKET = os.environ["S3_BUCKET_BRONZE"]
REGIONS = os.environ.get("YOUTUBE_REGIONS", "US,GB,CA,DE,FR,IN,JP,KR,MX,RU").split(",")
SNS_TOPIC = os.environ.get("SNS_ALERT_TOPIC_ARN", "")
API_BASE = "https://www.googleapis.com/youtube/v3"
MAX_RESULTS = 50


def fetch_trending_videos(region_code: str) -> dict:
    """Call the YouTube Data API to get trending videos for a given region.
    region_code must be UPPERCASE — this is what the API expects."""
    params = urlencode({
        "part": "snippet,statistics,contentDetails",
        "chart": "mostPopular",
        "regionCode": region_code,
        "maxResults": MAX_RESULTS,
        "key": API_KEY,
    })
    req = Request(f"{API_BASE}/videos?{params}", headers={"Accept": "application/json"})
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_video_categories(region_code: str) -> dict:
    """Fetch the video category mapping for a region.
    region_code must be UPPERCASE — this is what the API expects."""
    params = urlencode({
        "part": "snippet",
        "regionCode": region_code,
        "key": API_KEY,
    })
    req = Request(f"{API_BASE}/videoCategories?{params}", headers={"Accept": "application/json"})
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def write_to_s3(data: dict, bucket: str, key: str) -> dict:
    """Write JSON data to S3 with metadata."""
    body = json.dumps(data, ensure_ascii=False, indent=2)
    return s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
        Metadata={
            "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "youtube_data_api_v3",
        },
    )


def send_alert(subject: str, message: str):
    """Send failure alert via SNS."""
    if SNS_TOPIC:
        sns_client.publish(
            TopicArn=SNS_TOPIC,
            Subject=subject[:100],
            Message=message,
        )


def lambda_handler(event, context):
    """
    Main handler. Iterates over regions, fetches trending videos
    and category mappings, writes everything to Bronze layer.
    """
    now = datetime.now(timezone.utc)
    date_partition = now.strftime("%Y-%m-%d")
    hour_partition = now.strftime("%H")
    ingestion_id = now.strftime("%Y%m%d_%H%M%S")

    results = {"success": [], "failed": []}

    for raw_region in REGIONS:
        # FIX: Keep two casings, used for two different purposes.
        #   - api_region   -> UPPERCASE, required by the YouTube Data API v3 `regionCode` param
        #   - s3_region    -> lowercase, used consistently for S3 partition keys / paths
        # Previously a single `region = region.strip().upper()` variable was reused for
        # BOTH the API call and the S3 key, so partitions ended up split across
        # region=GB/ and region=gb/ (or region=IN/ and region=in/) depending on whatever
        # casing happened to be in YOUTUBE_REGIONS or leftover from earlier runs.
        api_region = raw_region.strip().upper()
        s3_region = raw_region.strip().lower()
        logger.info(f"Processing region: {api_region} (S3 partition: region={s3_region})")

        # ── Fetch trending videos ────────────────────────────────────────
        try:
            trending_data = fetch_trending_videos(api_region)
            video_count = len(trending_data.get("items", []))

            trending_data["_pipeline_metadata"] = {
                "ingestion_id": ingestion_id,
                "region": api_region,
                "ingestion_timestamp": now.isoformat(),
                "video_count": video_count,
                "source": "youtube_data_api_v3",
            }

            # s3://bucket/youtube/raw_statistics/region=gb/date=2026-08-20/hour=05/20260820_050000.json
            s3_key = (
                f"youtube/raw_statistics/"
                f"region={s3_region}/"
                f"date={date_partition}/"
                f"hour={hour_partition}/"
                f"{ingestion_id}.json"
            )
            write_to_s3(trending_data, BUCKET, s3_key)
            logger.info(f"  Wrote {video_count} videos → s3://{BUCKET}/{s3_key}")

        except (HTTPError, URLError) as e:
            logger.error(f"  API error for {api_region} trending: {e}")
            results["failed"].append({"region": s3_region, "type": "trending", "error": str(e)})
            continue
        except Exception as e:
            logger.error(f"  Unexpected error for {api_region} trending: {e}")
            results["failed"].append({"region": s3_region, "type": "trending", "error": str(e)})
            continue

        # ── Fetch category reference data ────────────────────────────────
        try:
            category_data = fetch_video_categories(api_region)
            category_data["_pipeline_metadata"] = {
                "ingestion_id": ingestion_id,
                "region": api_region,
                "ingestion_timestamp": now.isoformat(),
                "source": "youtube_data_api_v3",
            }

            ref_key = (
                f"youtube/raw_statistics_reference_data/"
                f"region={s3_region}/"
                f"date={date_partition}/"
                f"{s3_region}_category_id.json"
            )
            write_to_s3(category_data, BUCKET, ref_key)
            logger.info(f"  Wrote categories → s3://{BUCKET}/{ref_key}")

        except (HTTPError, URLError) as e:
            logger.error(f"  API error for {api_region} categories: {e}")
            results["failed"].append({"region": s3_region, "type": "categories", "error": str(e)})
        except Exception as e:
            logger.error(f"  Unexpected error for {api_region} categories: {e}")
            results["failed"].append({"region": s3_region, "type": "categories", "error": str(e)})

        results["success"].append(s3_region)

    # ── Summary & Alerting ───────────────────────────────────────────────
    summary = (
        f"Ingestion {ingestion_id} complete. "
        f"Success: {len(results['success'])}/{len(REGIONS)} regions. "
        f"Failed: {len(results['failed'])}."
    )
    logger.info(summary)

    if results["failed"]:
        send_alert(
            subject=f"[YT Pipeline] Ingestion partial failure — {ingestion_id}",
            message=json.dumps(results, indent=2),
        )

    return {
        "statusCode": 200,
        "ingestion_id": ingestion_id,
        "results": results,
    }
