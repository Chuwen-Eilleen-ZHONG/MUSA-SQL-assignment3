# Assignment 03 Responses

## Part 4: BigQuery External Tables

### Hourly Observations — CSV External Table SQL

```sql
CREATE EXTERNAL TABLE `sql-project1-487819.air_quality.hourly_observations_csv`
OPTIONS (
  format = 'CSV',
  uris = ['gs://musa5090-s26-chuwen-data/air_quality/hourly/*.csv'],
  skip_leading_rows = 1
);
```

### Hourly Observations — JSON-L External Table SQL

```sql
CREATE EXTERNAL TABLE `sql-project1-487819.air_quality.hourly_observations_jsonl`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://musa5090-s26-chuwen-data/air_quality/hourly/*.jsonl']
);
```

### Hourly Observations — Parquet External Table SQL

```sql
CREATE EXTERNAL TABLE `sql-project1-487819.air_quality.hourly_observations_parquet`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://musa5090-s26-chuwen-data/air_quality/hourly/*.parquet']
);
```

### Site Locations — CSV External Table SQL

```sql
CREATE EXTERNAL TABLE `sql-project1-487819.air_quality.site_locations_csv`
OPTIONS (
  format = 'CSV',
  uris = ['gs://musa5090-s26-chuwen-data/air_quality/sites/site_locations.csv'],
  skip_leading_rows = 1
);
```

### Site Locations — JSON-L External Table SQL

```sql
CREATE EXTERNAL TABLE `sql-project1-487819.air_quality.site_locations_jsonl`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://musa5090-s26-chuwen-data/air_quality/sites/site_locations.jsonl']
);
```

### Site Locations — GeoParquet External Table SQL

```sql
CREATE EXTERNAL TABLE `sql-project1-487819.air_quality.site_locations_geoparquet`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://musa5090-s26-chuwen-data/air_quality/sites/site_locations.geoparquet']
);
```

### Cross-Table Join Query

```sql
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
```

---

## Part 5: Hive-Partitioned External Tables

### Hourly Observations — CSV (hive-partitioned)
```sql
CREATE EXTERNAL TABLE `sql-project1-487819.air_quality.hourly_observations_csv_partitioned`
WITH PARTITION COLUMNS (airnow_date DATE)
OPTIONS (
  format = 'CSV',
  uris = ['gs://musa5090-s26-chuwen-data/air_quality/hourly/csv/*'],
  skip_leading_rows = 1,
  hive_partition_uri_prefix = 'gs://musa5090-s26-chuwen-data/air_quality/hourly/csv'
);
```

### Hourly Observations — JSON-L (hive-partitioned)
```sql
CREATE EXTERNAL TABLE `sql-project1-487819.air_quality.hourly_observations_jsonl_partitioned`
WITH PARTITION COLUMNS (airnow_date DATE)
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://musa5090-s26-chuwen-data/air_quality/hourly/jsonl/*'],
  hive_partition_uri_prefix = 'gs://musa5090-s26-chuwen-data/air_quality/hourly/jsonl'
);
```

### Hourly Observations — Parquet (hive-partitioned)
```sql
CREATE EXTERNAL TABLE `sql-project1-487819.air_quality.hourly_observations_parquet_partitioned`
WITH PARTITION COLUMNS (airnow_date DATE)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://musa5090-s26-chuwen-data/air_quality/hourly/parquet/*'],
  hive_partition_uri_prefix = 'gs://musa5090-s26-chuwen-data/air_quality/hourly/parquet'
);
```

---

## Part 6: Analysis & Reflection

### 1. File Sizes

**Hourly data (single day):**

| Format  | File Size |
|---------|-----------|
| CSV     |   17.6 MB   |
| JSON-L  |   41.6 MB   |
| Parquet |   796 KB    |

**Site locations:**

| Format     | File Size |
|------------|-----------|
| CSV        |   0.98 MB   |
| JSON-L     |   2.79 MB   |
| GeoParquet |   474 KB    |

**Analysis:**
> Parquet is the smallest because it uses columnar storage and built-in compression. JSON-L is the largest because it repeats every column name as a key for every single row, which adds a lot of redundant text. CSV is in the middle — it stores data as plain text without column name repetition.

### 2. Format Anatomy

> CSV stores data as plain text rows separated by commas (or pipes in our case), with one row per observation. It is human-readable and widely supported, but has no built-in data types — everything is a string until parsed.
Parquet is a binary columnar format, meaning it stores all values for a single column together rather than row by row. This makes it much faster to query when you only need a few columns, and allows for efficient compression. It also stores data type information natively.


### 3. Choosing Formats for BigQuery

> Parquet is preferred over CSV or JSON-L for BigQuery external tables for two main reasons. First, performance: BigQuery only needs to read the columns referenced in a query rather than scanning entire rows, which is much faster for analytical queries. Second, cost: BigQuery charges by the amount of data scanned — since Parquet files are much smaller due to compression, queries cost significantly less.

### 4. Pipeline vs. Warehouse Joins

> Joining at query time (keeping tables separate) is more flexible — you can update site location data independently without reprocessing all hourly observations, and you avoid duplicating coordinate data across millions of rows. This is better when data changes frequently or storage cost matters.
Joining during the Prepare step (denormalized) makes queries simpler and faster since no join is needed at query time. This is better when query performance is critical and the data is relatively stable.
For this assignment, keeping them separate is the better approach because site locations rarely change, and joining in BigQuery is cheap and fast.

#### Stretch Challenge (optional)

If you implemented the stretch challenge (scripts `06_prepare`, `06_upload_to_gcs`, `06_create_tables.sql`), paste your SQL statements here:

```sql
-- Merged Hourly + Sites — CSV (hive-partitioned)
```

```sql
-- Merged Hourly + Sites — JSON-L (hive-partitioned)
```

```sql
-- Merged Hourly + Sites — GeoParquet (hive-partitioned)
```

### 5. Choosing a Data Source

For each person below, which air quality data source (AirNow hourly files, AirNow API, AQS bulk downloads, or AQS API) would you recommend, and why?

**a) A parent who wants a dashboard showing current air quality near their child's school:**
> Parent wanting current air quality near a school: AirNow API — it provides real-time data and is designed for exactly this kind of consumer-facing use case where up-to-the-hour readings matter.

**b) An environmental justice advocate identifying neighborhoods with chronically poor air quality over the past decade:**
> Environmental justice advocate studying decade-long trends: AQS bulk downloads — they contain the full historical record going back decades with detailed metadata, ideal for long-term trend analysis.

**c) A school administrator who needs automated morning alerts when AQI exceeds a threshold:**
> School administrator needing automated morning alerts: AirNow API — it supports automated querying and provides current AQI values that can be checked each morning to trigger alerts when thresholds are exceeded.
