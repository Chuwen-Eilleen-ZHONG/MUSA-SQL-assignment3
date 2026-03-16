"""
    Stretch challenge: Upload merged hourly + site location data to GCS.

    This script uploads the denormalized (merged) files produced by
    06_prepare.py to GCS with a hive-partitioned folder structure.

    Prerequisites:
        - Run `gcloud auth application-default login` to authenticate.
        - Part 6 prepare script (06_prepare.py) should be complete.

    Usage:
        python scripts/06_upload_to_gcs.py
"""

import pathlib

from google.cloud import storage


DATA_DIR = pathlib.Path(__file__).parent.parent / 'data'

BUCKET_NAME = 'musa5090-s26-chuwen-data'

EXTENSIONS = ('csv', 'jsonl', 'geoparquet')


def upload_merged_data():
    """Upload merged hourly data to GCS with hive-partitioned folder structure.

    Expected GCS structure:
        gs://<bucket>/air_quality/hourly_with_sites/csv/airnow_date=2024-07-01/data.csv
        gs://<bucket>/air_quality/hourly_with_sites/jsonl/airnow_date=2024-07-01/data.jsonl
        gs://<bucket>/air_quality/hourly_with_sites/geoparquet/airnow_date=2024-07-01/data.geoparquet
    """
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    hourly_with_sites_dir = DATA_DIR / 'prepared' / 'hourly_with_sites'

    date_stems = set()
    for ext in EXTENSIONS:
        for f in hourly_with_sites_dir.glob(f'*.{ext}'):
            date_stems.add(f.stem)

    for date_stem in sorted(date_stems):
        for ext in EXTENSIONS:
            local_file = hourly_with_sites_dir / f'{date_stem}.{ext}'
            if not local_file.exists():
                continue

            blob_name = f'air_quality/hourly_with_sites/{ext}/airnow_date={date_stem}/data.{ext}'
            blob = bucket.blob(blob_name)
            blob.upload_from_filename(str(local_file))
            print(f'Uploaded {local_file.name} → gs://{BUCKET_NAME}/{blob_name}')


if __name__ == '__main__':
    upload_merged_data()
    print('Done.')
