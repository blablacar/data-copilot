-- Detailed temporal patterns: day of week, hour of day analysis
-- Focus on human contributors to understand when active editors contribute

WITH recent_human_edits AS (
  SELECT
    revision_id,
    contributor_id,
    contributor_username,
    TIMESTAMP_SECONDS(timestamp) AS edit_timestamp,
    EXTRACT(YEAR FROM TIMESTAMP_SECONDS(timestamp)) AS edit_year,
    EXTRACT(MONTH FROM TIMESTAMP_SECONDS(timestamp)) AS edit_month,
    EXTRACT(DAYOFWEEK FROM TIMESTAMP_SECONDS(timestamp)) AS day_of_week,
    EXTRACT(HOUR FROM TIMESTAMP_SECONDS(timestamp)) AS hour_of_day,
    is_minor,
    is_bot,
    num_characters
  FROM
    `Your-Project-ID-Here.samples.wikipedia`
  WHERE
    timestamp IS NOT NULL
    AND contributor_id IS NOT NULL
    AND (is_bot IS NULL OR is_bot = FALSE)
)

-- Day of week pattern
SELECT
  'DAY_OF_WEEK' AS pattern_type,
  day_of_week AS time_dimension,
  CASE day_of_week
    WHEN 1 THEN 'Sunday'
    WHEN 2 THEN 'Monday'
    WHEN 3 THEN 'Tuesday'
    WHEN 4 THEN 'Wednesday'
    WHEN 5 THEN 'Thursday'
    WHEN 6 THEN 'Friday'
    WHEN 7 THEN 'Saturday'
  END AS time_label,
  COUNT(*) AS total_edits,
  COUNT(DISTINCT contributor_id) AS unique_contributors,
  SUM(CASE WHEN is_minor = TRUE THEN 1 ELSE 0 END) AS minor_edits,
  AVG(num_characters) AS avg_article_length
FROM recent_human_edits
GROUP BY day_of_week

UNION ALL

-- Hour of day pattern
SELECT
  'HOUR_OF_DAY' AS pattern_type,
  hour_of_day AS time_dimension,
  CAST(hour_of_day AS STRING) AS time_label,
  COUNT(*) AS total_edits,
  COUNT(DISTINCT contributor_id) AS unique_contributors,
  SUM(CASE WHEN is_minor = TRUE THEN 1 ELSE 0 END) AS minor_edits,
  AVG(num_characters) AS avg_article_length
FROM recent_human_edits
GROUP BY hour_of_day

UNION ALL

-- Monthly trend
SELECT
  'MONTHLY_TREND' AS pattern_type,
  edit_year * 100 + edit_month AS time_dimension,
  CONCAT(CAST(edit_year AS STRING), '-', LPAD(CAST(edit_month AS STRING), 2, '0')) AS time_label,
  COUNT(*) AS total_edits,
  COUNT(DISTINCT contributor_id) AS unique_contributors,
  SUM(CASE WHEN is_minor = TRUE THEN 1 ELSE 0 END) AS minor_edits,
  AVG(num_characters) AS avg_article_length
FROM recent_human_edits
GROUP BY edit_year, edit_month

ORDER BY pattern_type, time_dimension
