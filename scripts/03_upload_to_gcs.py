"""
    Script to upload prepared data files to Google Cloud Storage (GCS).

    This script uploads the transformed files from data/prepared/ to a
    GCS bucket, preserving the folder structure so that BigQuery can
    use wildcard URIs to create external tables across multiple files.

    Prerequisites:
        - Run `gcloud auth application-default login` to authenticate.
        - Create a GCS bucket (manually or in this script).

    Usage:
        python scripts/03_upload_to_gcs.py
"""

import pathlib

from google.cloud import storage
from google.cloud.exceptions import Conflict


DATA_DIR = pathlib.Path(__file__).parent.parent / 'data'

BUCKET_NAME = 'musa5090-s26-chuwen-data'
PROJECT_ID = 'sql-project1-487819'


def upload_prepared_data():
    """Upload all prepared data files to GCS.

    Uploads the contents of data/prepared/ to the GCS bucket,
    preserving the folder structure under a prefix of 'air_quality/'.

    Expected GCS structure:
        gs://<bucket>/air_quality/hourly/2024-07-01.csv
        gs://<bucket>/air_quality/hourly/2024-07-01.jsonl
        gs://<bucket>/air_quality/hourly/2024-07-01.parquet
        ...
        gs://<bucket>/air_quality/sites/site_locations.csv
        gs://<bucket>/air_quality/sites/site_locations.jsonl
        gs://<bucket>/air_quality/sites/site_locations.geoparquet
    """
    client = storage.Client(project=PROJECT_ID)

    # Create the bucket if it doesn't exist
    bucket = client.bucket(BUCKET_NAME)
    try:
        bucket = client.create_bucket(bucket, project=PROJECT_ID)
        print(f'Created bucket {BUCKET_NAME}')
    except Conflict:
        bucket = client.get_bucket(BUCKET_NAME)
        print(f'Bucket {BUCKET_NAME} already exists')

    # Upload hourly and sites folders
    prepared_dir = DATA_DIR / 'prepared'
    for subfolder in ('hourly', 'sites'):
        local_dir = prepared_dir / subfolder
        if not local_dir.exists():
            print(f'Directory {local_dir} does not exist, skipping.')
            continue
        for local_file in local_dir.iterdir():
            if not local_file.is_file():
                continue
            blob_name = f'air_quality/{subfolder}/{local_file.name}'
            blob = bucket.blob(blob_name)
            blob.upload_from_filename(str(local_file))
            print(f'Uploaded {local_file} to gs://{BUCKET_NAME}/{blob_name}')


if __name__ == '__main__':
    upload_prepared_data()
    print('Done.')
