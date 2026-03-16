"""
    Script to re-upload prepared data to GCS using hive-partitioned folder structure.

    This script takes the same prepared files from Part 2 and uploads them
    to GCS with a hive-partitioned directory layout. Instead of flat files like:
        air_quality/hourly/2024-07-01.csv

    Files are organized as:
        air_quality/hourly/csv/airnow_date=2024-07-01/data.csv

    This enables BigQuery to automatically detect the partition key
    (airnow_date) and use it for query pruning, so queries filtering
    by date only scan the relevant files.

    This is a backfill of the upload step — you don't need to re-download
    or re-transform anything. You're just re-uploading the same files
    with a different folder structure.

    Prerequisites:
        - Run `gcloud auth application-default login` to authenticate.
        - Parts 1-3 should be complete (data already prepared and uploaded once).

    Usage:
        python scripts/05_upload_to_gcs.py
"""

import pathlib

from google.cloud import storage


DATA_DIR = pathlib.Path(__file__).parent.parent / 'data'

BUCKET_NAME = 'musa5090-s26-chuwen-data'

EXTENSIONS = ('csv', 'jsonl', 'parquet')


def upload_with_hive_partitioning():
    """Upload prepared hourly data to GCS with hive-partitioned folder structure.

    For each date's prepared files, upload them to GCS with the following
    folder structure:
        gs://<bucket>/air_quality/hourly/csv/airnow_date=2024-07-01/data.csv
        gs://<bucket>/air_quality/hourly/jsonl/airnow_date=2024-07-01/data.jsonl
        gs://<bucket>/air_quality/hourly/parquet/airnow_date=2024-07-01/data.parquet

    The site locations files don't need hive partitioning (they're not
    date-partitioned), so you can re-upload them as-is or skip them.
    """
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    hourly_dir = DATA_DIR / 'prepared' / 'hourly'

    # Collect all date stems from any of the supported extensions
    date_stems = set()
    for ext in EXTENSIONS:
        for f in hourly_dir.glob(f'*.{ext}'):
            date_stems.add(f.stem)

    for date_stem in sorted(date_stems):
        for ext in EXTENSIONS:
            local_file = hourly_dir / f'{date_stem}.{ext}'
            if not local_file.exists():
                continue

            blob_name = f'air_quality/hourly/{ext}/airnow_date={date_stem}/data.{ext}'
            blob = bucket.blob(blob_name)
            blob.upload_from_filename(str(local_file))
            print(f'Uploaded {local_file.name} → gs://{BUCKET_NAME}/{blob_name}')


if __name__ == '__main__':
    upload_with_hive_partitioning()
    print('Done.')
