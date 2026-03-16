-- Part 4: Create BigQuery external tables
--
-- Create these tables in a dataset named `air_quality`.
-- Use wildcard URIs for the hourly data tables so a single table
-- spans all 31 days of files.
--
-- After creating the tables, verify they work by running:
--     SELECT count(*) FROM air_quality.<table_name>;


-- Hourly Observations — CSV
-- TODO: Create external table `hourly_observations_csv`
-- pointing to gs://<your-bucket>/air_quality/hourly/*.csv
CREATE EXTERNAL TABLE `sql-project1-487819.air_quality.hourly_observations_csv`
OPTIONS (
  format = 'CSV',
  uris = ['gs://musa5090-s26-chuwen-data/air_quality/hourly/*.csv'],
  skip_leading_rows = 1
);

-- Hourly Observations — JSON-L
-- TODO: Create external table `hourly_observations_jsonl`
-- pointing to gs://<your-bucket>/air_quality/hourly/*.jsonl
CREATE EXTERNAL TABLE `sql-project1-487819.air_quality.hourly_observations_jsonl`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://musa5090-s26-chuwen-data/air_quality/hourly/*.jsonl']
);

-- Hourly Observations — Parquet
-- TODO: Create external table `hourly_observations_parquet`
-- pointing to gs://<your-bucket>/air_quality/hourly/*.parquet
CREATE EXTERNAL TABLE `sql-project1-487819.air_quality.hourly_observations_parquet`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://musa5090-s26-chuwen-data/air_quality/hourly/*.parquet']
);

-- Site Locations — CSV
-- TODO: Create external table `site_locations_csv`
-- pointing to gs://<your-bucket>/air_quality/sites/site_locations.csv
CREATE EXTERNAL TABLE `sql-project1-487819.air_quality.site_locations_csv`
OPTIONS (
  format = 'CSV',
  uris = ['gs://musa5090-s26-chuwen-data/air_quality/sites/site_locations.csv'],
  skip_leading_rows = 1
);

-- Site Locations — JSON-L
-- TODO: Create external table `site_locations_jsonl`
-- pointing to gs://<your-bucket>/air_quality/sites/site_locations.jsonl
CREATE EXTERNAL TABLE `sql-project1-487819.air_quality.site_locations_jsonl`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://musa5090-s26-chuwen-data/air_quality/sites/site_locations.jsonl']
);

-- Site Locations — GeoParquet
-- TODO: Create external table `site_locations_geoparquet`
-- pointing to gs://<your-bucket>/air_quality/sites/site_locations.geoparquet
CREATE EXTERNAL TABLE `sql-project1-487819.air_quality.site_locations_geoparquet`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://musa5090-s26-chuwen-data/air_quality/sites/site_locations.geoparquet']
);

-- Cross-table join query
-- Write a query that joins hourly observations with site locations
-- to get latitude/longitude for each observation. For example,
-- find the average PM2.5 value by state for a single day.
SELECT 
    o.valid_date,
    o.valid_time,
    o.site_name,
    o.parameter_name,
    o.value,
    o.reporting_units,
    s.Latitude,
    s.Longitude,
    s.StateAbbreviation
FROM `sql-project1-487819.air_quality.hourly_observations_csv` o
JOIN `sql-project1-487819.air_quality.site_locations_csv` s
    ON o.aqsid = s.AQSID
WHERE o.parameter_name = 'PM2.5'
    AND o.valid_date = '2024-07-01'
ORDER BY s.StateAbbreviation, o.value DESC
LIMIT 100;