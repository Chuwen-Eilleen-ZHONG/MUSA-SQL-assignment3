-- Part 6 (stretch challenge): Create BigQuery external tables for merged data
--
-- These tables point to the denormalized files where hourly observations
-- have been pre-joined with site location data during the prepare step.
-- Each observation row already includes latitude, longitude, state, etc.
--
-- Use hive partitioning with the airnow_date partition key.


-- Merged Hourly + Sites — CSV (hive-partitioned)
-- TODO: Create external table `hourly_with_sites_csv`
-- pointing to gs://<your-bucket>/air_quality/hourly_with_sites/csv/*
-- with hive partitioning options
CREATE EXTERNAL TABLE `sql-project1-487819.air_quality.hourly_with_sites_csv`
WITH PARTITION COLUMNS (airnow_date DATE)
OPTIONS (
  format = 'CSV',
  uris = ['gs://musa5090-s26-chuwen-data/air_quality/hourly_with_sites/csv/*'],
  skip_leading_rows = 1,
  hive_partition_uri_prefix = 'gs://musa5090-s26-chuwen-data/air_quality/hourly_with_sites/csv'
);

-- Merged Hourly + Sites — JSON-L (hive-partitioned)
-- TODO: Create external table `hourly_with_sites_jsonl`
-- pointing to gs://<your-bucket>/air_quality/hourly_with_sites/jsonl/*
-- with hive partitioning options
CREATE EXTERNAL TABLE `sql-project1-487819.air_quality.hourly_with_sites_jsonl`
WITH PARTITION COLUMNS (airnow_date DATE)
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://musa5090-s26-chuwen-data/air_quality/hourly_with_sites/jsonl/*'],
  hive_partition_uri_prefix = 'gs://musa5090-s26-chuwen-data/air_quality/hourly_with_sites/jsonl'
);

-- Merged Hourly + Sites — GeoParquet (hive-partitioned)
-- TODO: Create external table `hourly_with_sites_geoparquet`
-- pointing to gs://<your-bucket>/air_quality/hourly_with_sites/geoparquet/*
-- with hive partitioning options
CREATE EXTERNAL TABLE `sql-project1-487819.air_quality.hourly_with_sites_geoparquet`
WITH PARTITION COLUMNS (airnow_date DATE)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://musa5090-s26-chuwen-data/air_quality/hourly_with_sites/geoparquet/*'],
  hive_partition_uri_prefix = 'gs://musa5090-s26-chuwen-data/air_quality/hourly_with_sites/geoparquet'
);