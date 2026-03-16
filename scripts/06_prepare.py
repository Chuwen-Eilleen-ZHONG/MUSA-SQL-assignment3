"""
    Stretch challenge: Prepare merged hourly + site location data.

    This script joins the hourly observation data with site location data
    during the prepare step (denormalization), producing files where
    each observation row includes the site's latitude, longitude, and
    other geographic metadata.

    This is the alternative to the approach in Part 4, where hourly data
    and site locations were kept as separate tables and joined at query
    time in BigQuery.

    This is a backfill of the prepare step — you're re-processing the
    same raw data you already downloaded, but with a different
    transformation that produces a richer output.

    Usage:
        python scripts/06_prepare.py
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
    df = pd.read_csv(
        most_recent / 'Monitoring_Site_Locations_V2.dat',
        sep='|', header=0, encoding='latin-1',
    )
    df = df.drop_duplicates(subset='AQSID', keep='first')
    return df


def _merge(date_str):
    """Load hourly data and site locations, join on aqsid == AQSID."""
    hourly = _load_hourly(date_str)
    sites = _load_site_locations()
    merged = hourly.merge(sites, left_on='aqsid', right_on='AQSID', how='left')
    merged = merged.drop(columns=['AQSID'])
    return merged


def prepare_merged_csv(date_str):
    """Merge hourly observations with site locations and write as CSV.

    Reads the hourly .dat files for the given date and the site locations
    file, joins them on AQSID, and writes to
    data/prepared/hourly_with_sites/<date>.csv.

    Args:
        date_str: Date string in 'YYYY-MM-DD' format.
    """
    out_path = DATA_DIR / 'prepared' / 'hourly_with_sites' / f'{date_str}.csv'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = _merge(date_str)
    df.to_csv(out_path, index=False)


def prepare_merged_jsonl(date_str):
    """Merge hourly observations with site locations and write as JSON-L.

    Reads the hourly .dat files for the given date and the site locations
    file, joins them on AQSID, and writes to
    data/prepared/hourly_with_sites/<date>.jsonl.

    Args:
        date_str: Date string in 'YYYY-MM-DD' format.
    """
    out_path = DATA_DIR / 'prepared' / 'hourly_with_sites' / f'{date_str}.jsonl'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = _merge(date_str)
    df.to_json(out_path, orient='records', lines=True)


def prepare_merged_geoparquet(date_str):
    """Merge hourly observations with site locations and write as GeoParquet.

    Reads the hourly .dat files for the given date and the site locations
    file, joins them on AQSID, creates point geometries from the site's
    latitude and longitude, and writes to
    data/prepared/hourly_with_sites/<date>.geoparquet.

    Args:
        date_str: Date string in 'YYYY-MM-DD' format.
    """
    out_path = DATA_DIR / 'prepared' / 'hourly_with_sites' / f'{date_str}.geoparquet'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = _merge(date_str)
    geometry = [
        Point(lon, lat) if pd.notna(lon) and pd.notna(lat) else None
        for lon, lat in zip(df['Longitude'], df['Latitude'])
    ]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')
    gdf.to_parquet(out_path, index=False)


if __name__ == '__main__':
    import datetime

    # Backfill: prepare merged data for each day in July 2024
    start_date = datetime.date(2024, 7, 1)
    end_date = datetime.date(2024, 7, 31)

    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.isoformat()
        print(f'Preparing merged data for {date_str}...')
        prepare_merged_csv(date_str)
        prepare_merged_jsonl(date_str)
        prepare_merged_geoparquet(date_str)
        current_date += datetime.timedelta(days=1)

    print('Done.')
