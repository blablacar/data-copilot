-- Inspect min/max timestamps and corresponding years in the wikipedia table
SELECT
  MIN(TIMESTAMP_SECONDS(timestamp)) AS min_edit_ts,
  MAX(TIMESTAMP_SECONDS(timestamp)) AS max_edit_ts,
  EXTRACT(YEAR FROM MIN(TIMESTAMP_SECONDS(timestamp))) AS min_year,
  EXTRACT(YEAR FROM MAX(TIMESTAMP_SECONDS(timestamp))) AS max_year,
  COUNT(*) AS total_rows
FROM `Your-Project-ID-Here.samples.wikipedia`;
