"""
    Script to transform raw AirNow data files into BigQuery-compatible formats.

    This script reads the raw .dat files downloaded by 01_extract.py and converts
    them into CSV, JSON-L, and Parquet formats suitable for loading into
    BigQuery as external tables.

    Hourly observation data is converted to: CSV, JSON-L, Parquet
    Site location data is converted to: CSV, JSON-L, GeoParquet (with point geometry)

    Usage:
        python scripts/02_prepare.py
"""

import pathlib

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point


DATA_DIR = pathlib.Path(__file__).parent.parent / 'data'

HOURLY_COLUMNS = [
    'valid_date',
    'valid_time',
    'aqsid',
    'site_name',
    'gmt_offset',
    'parameter_name',
    'reporting_units',
    'value',
    'data_source',
]


# --- Private helpers ---

def _load_hourly(date_str):
    """Read all 24 HourlyData_*.dat files for a date and return a combined DataFrame."""
    raw_dir = DATA_DIR / 'raw' / date_str
    files = sorted(raw_dir.glob('HourlyData_*.dat'))
    frames = [
        pd.read_csv(f, sep='|', header=None, names=HOURLY_COLUMNS, encoding='latin-1')
        for f in files
    ]
    return pd.concat(frames, ignore_index=True)


def _load_site_locations():
    """Read Monitoring_Site_Locations_V2.dat from the most recent date and deduplicate by AQSID."""
    raw_dir = DATA_DIR / 'raw'
    date_dirs = sorted(d for d in raw_dir.iterdir() if d.is_dir())
    most_recent = date_dirs[-1]
    df = pd.read_csv(most_recent / 'Monitoring_Site_Locations_V2.dat', sep='|', header=0)
    df = df.drop_duplicates(subset='AQSID', keep='first')
    return df


# --- Hourly observation data ---

def prepare_hourly_csv(date_str):
    """Convert raw hourly .dat files for a date to a single CSV file.

    Reads all 24 HourlyData_*.dat files from data/raw/<date>/,
    combines them into a single dataset, assigns column names,
    and writes to data/prepared/hourly/<date>.csv.

    Args:
        date_str: Date string in 'YYYY-MM-DD' format.
    """
    out_path = DATA_DIR / 'prepared' / 'hourly' / f'{date_str}.csv'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = _load_hourly(date_str)
    df.to_csv(out_path, index=False)


def prepare_hourly_jsonl(date_str):
    """Convert raw hourly .dat files for a date to newline-delimited JSON.

    Reads all 24 HourlyData_*.dat files from data/raw/<date>/,
    combines them, and writes one JSON object per line to
    data/prepared/hourly/<date>.jsonl.

    Args:
        date_str: Date string in 'YYYY-MM-DD' format.
    """
    out_path = DATA_DIR / 'prepared' / 'hourly' / f'{date_str}.jsonl'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = _load_hourly(date_str)
    df.to_json(out_path, orient='records', lines=True)


def prepare_hourly_parquet(date_str):
    """Convert raw hourly .dat files for a date to Parquet format.

    Reads all 24 HourlyData_*.dat files from data/raw/<date>/,
    combines them, and writes to data/prepared/hourly/<date>.parquet.

    Args:
        date_str: Date string in 'YYYY-MM-DD' format.
    """
    out_path = DATA_DIR / 'prepared' / 'hourly' / f'{date_str}.parquet'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = _load_hourly(date_str)
    df.to_parquet(out_path, index=False)


# --- Site location data ---

def prepare_site_locations_csv():
    """Convert monitoring site locations to CSV.

    Reads the Monitoring_Site_Locations_V2.dat file, deduplicates
    so there is one row per site (the raw file has one row per
    site-parameter combination), and writes to
    data/prepared/sites/site_locations.csv.

    Use the most recent date's file from data/raw/.
    """
    out_path = DATA_DIR / 'prepared' / 'sites' / 'site_locations.csv'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = _load_site_locations()
    df.to_csv(out_path, index=False)


def prepare_site_locations_jsonl():
    """Convert monitoring site locations to newline-delimited JSON.

    Reads the Monitoring_Site_Locations_V2.dat file, deduplicates
    so there is one row per site (the raw file has one row per
    site-parameter combination), and writes to
    data/prepared/sites/site_locations.jsonl.

    Use the most recent date's file from data/raw/.
    """
    out_path = DATA_DIR / 'prepared' / 'sites' / 'site_locations.jsonl'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = _load_site_locations()
    df.to_json(out_path, orient='records', lines=True)


def prepare_site_locations_geoparquet():
    """Convert monitoring site locations to GeoParquet with point geometry.

    Reads the Monitoring_Site_Locations_V2.dat file, deduplicates
    so there is one row per site (the raw file has one row per
    site-parameter combination), creates point geometries from
    latitude and longitude, and writes to
    data/prepared/sites/site_locations.geoparquet.

    Use the most recent date's file from data/raw/.
    """
    out_path = DATA_DIR / 'prepared' / 'sites' / 'site_locations.geoparquet'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = _load_site_locations()
    geometry = [
        Point(lon, lat)
        for lon, lat in zip(df['Longitude'], df['Latitude'])
    ]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')
    gdf.to_parquet(out_path, index=False)


if __name__ == '__main__':
    import datetime

    # Prepare site locations (only need to do this once)
    print('Preparing site locations...')
    prepare_site_locations_csv()
    prepare_site_locations_jsonl()
    prepare_site_locations_geoparquet()

    # Prepare hourly data for each day in July 2024 (backfill)
    start_date = datetime.date(2024, 7, 1)
    end_date = datetime.date(2024, 7, 31)

    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.isoformat()
        print(f'Preparing hourly data for {date_str}...')
        prepare_hourly_csv(date_str)
        prepare_hourly_jsonl(date_str)
        prepare_hourly_parquet(date_str)
        current_date += datetime.timedelta(days=1)

    print('Done.')
