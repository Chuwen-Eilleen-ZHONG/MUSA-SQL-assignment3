"""
    Script to extract AirNow data files for a range of dates.

    This script downloads hourly air quality observation data and monitoring
    site location data from the EPA's AirNow program. Files are saved into
    a date-organized folder structure under data/raw/.

    AirNow files are hosted at:
        https://files.airnowtech.org/?prefix=airnow/

    Usage:
        python scripts/01_extract.py
"""

import pathlib
import urllib.request


DATA_DIR = pathlib.Path(__file__).parent.parent / 'data'


def download_data_for_date(date_str):
    """Download AirNow data files for a single date.

    Downloads all 24 HourlyData files (hours 00-23) and the
    Monitoring_Site_Locations_V2.dat file for the specified date,
    saving them into data/raw/YYYY-MM-DD/.

    Args:
        date_str: Date string in 'YYYY-MM-DD' format. For example, '2024-07-01'.
    """
    date_compact = date_str.replace('-', '')
    year = date_str[:4]

    save_dir = DATA_DIR / 'raw' / date_str
    save_dir.mkdir(parents=True, exist_ok=True)

    for hour in range(24):
        hour_str = f'{hour:02d}'
        filename = f'HourlyData_{date_compact}{hour_str}.dat'
        url = f'https://s3-us-west-1.amazonaws.com/files.airnowtech.org/airnow/{year}/{date_compact}/{filename}'
        save_path = save_dir / filename

        if save_path.exists():
            print(f'  Already exists, skipping: {filename}')
            continue

        print(f'  Downloading: {filename}')
        urllib.request.urlretrieve(url, save_path)

    filename = 'Monitoring_Site_Locations_V2.dat'
    url = f'https://s3-us-west-1.amazonaws.com/files.airnowtech.org/airnow/{year}/{date_compact}/{filename}'
    save_path = save_dir / filename

    if not save_path.exists():
        print(f'  Downloading: {filename}')
        urllib.request.urlretrieve(url, save_path)

if __name__ == '__main__':
    import datetime

    # Download data for July 2024
    start_date = datetime.date(2024, 7, 1)
    end_date = datetime.date(2024, 7, 31)

    current_date = start_date
    while current_date <= end_date:
        print(f'Downloading data for {current_date}...')
        download_data_for_date(current_date.isoformat())
        current_date += datetime.timedelta(days=1)

    print('Done.')
