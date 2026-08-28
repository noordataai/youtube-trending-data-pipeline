"""
One-time cleanup: merge duplicate-case `region=` partitions in S3.

Problem:
    Your bucket has both region=GB/ and region=gb/ (same for IN/in, US/us, CA/ca...)
    because an earlier version of the ingestion Lambda used .upper() for both the
    YouTube API call AND the S3 key. This script finds every uppercase (or mixed-case)
    `region=XX/` prefix, copies its objects under the lowercase equivalent, and then
    deletes the old uppercase objects.

    Safe to re-run — if the lowercase key already exists it just gets overwritten
    with the same/latest content; nothing is skipped or duplicated.

How to run:
    This is a ONE-TIME migration, not a recurring Lambda. Run it from:
      - AWS CloudShell (easiest — already has your console credentials), or
      - Your local machine with `aws configure` set up, or
      - As a one-off Lambda invocation (paste into a scratch Lambda, run once, delete it)

    Local / CloudShell:
        pip install boto3   # usually already installed
        python migrate_region_partitions.py --bucket yt-pipeline-bronze-ap-south-1-dev --dry-run
        python migrate_region_partitions.py --bucket yt-pipeline-bronze-ap-south-1-dev

    Always run with --dry-run first to see exactly what will be copied/deleted
    before anything changes.

Environment Variables (optional, same as your Lambda):
    S3_BUCKET_BRONZE  — used as default bucket if --bucket is not passed
"""

import argparse
import os
import sys

import boto3

s3_client = boto3.client("s3")


def find_region_prefixes(bucket: str, base_prefix: str = "youtube/"):
    """
    Walk the bucket and find every distinct `region=XX/` segment
    (at any depth under base_prefix), grouped by the part of the key
    that comes AFTER the region segment stays the same, only the casing
    of XX differs.

    Returns a dict: { "youtube/raw_statistics/region=GB/": "youtube/raw_statistics/region=gb/", ... }
    mapping non-lowercase region prefixes -> their correct lowercase equivalent.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    mapping = {}

    for page in paginator.paginate(Bucket=bucket, Prefix=base_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if "region=" not in key:
                continue

            before, _, rest = key.partition("region=")
            region_code, _, after = rest.partition("/")

            if region_code != region_code.lower():
                old_prefix = f"{before}region={region_code}/"
                new_prefix = f"{before}region={region_code.lower()}/"
                mapping[old_prefix] = new_prefix

    return mapping


def migrate_prefix(bucket: str, old_prefix: str, new_prefix: str, dry_run: bool):
    """Copy every object under old_prefix to new_prefix, then delete the old ones."""
    paginator = s3_client.get_paginator("list_objects_v2")
    copied = 0
    deleted = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=old_prefix):
        for obj in page.get("Contents", []):
            old_key = obj["Key"]
            new_key = new_prefix + old_key[len(old_prefix):]

            if dry_run:
                print(f"  [DRY RUN] would copy  {old_key}  ->  {new_key}")
                print(f"  [DRY RUN] would delete {old_key}")
                continue

            s3_client.copy_object(
                Bucket=bucket,
                CopySource={"Bucket": bucket, "Key": old_key},
                Key=new_key,
            )
            copied += 1

            s3_client.delete_object(Bucket=bucket, Key=old_key)
            deleted += 1
            print(f"  moved {old_key} -> {new_key}")

    return copied, deleted


def main():
    parser = argparse.ArgumentParser(description="Merge duplicate-case region= partitions in S3.")
    parser.add_argument(
        "--bucket",
        default=os.environ.get("S3_BUCKET_BRONZE"),
        help="S3 bucket name (defaults to $S3_BUCKET_BRONZE if set)",
    )
    parser.add_argument(
        "--prefix",
        default="youtube/",
        help="Base prefix to scan under (default: youtube/)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would happen without copying/deleting anything",
    )
    args = parser.parse_args()

    if not args.bucket:
        print("ERROR: --bucket is required (or set S3_BUCKET_BRONZE env var).")
        sys.exit(1)

    print(f"Scanning s3://{args.bucket}/{args.prefix} for mixed-case region= partitions...\n")
    mapping = find_region_prefixes(args.bucket, args.prefix)

    if not mapping:
        print("No mixed-case region= partitions found. Nothing to do.")
        return

    print(f"Found {len(mapping)} prefix(es) to merge:")
    for old, new in mapping.items():
        print(f"  {old}  ->  {new}")
    print()

    total_copied = 0
    total_deleted = 0

    for old_prefix, new_prefix in mapping.items():
        print(f"Processing {old_prefix} ...")
        copied, deleted = migrate_prefix(args.bucket, old_prefix, new_prefix, args.dry_run)
        total_copied += copied
        total_deleted += deleted
        print()

    if args.dry_run:
        print("Dry run complete. Re-run without --dry-run to actually migrate.")
    else:
        print(f"Done. Copied {total_copied} object(s), deleted {total_deleted} old object(s).")


if __name__ == "__main__":
    main()
