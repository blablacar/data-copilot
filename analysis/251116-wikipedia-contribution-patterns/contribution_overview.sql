-- Wikipedia Contribution Patterns Overview
-- Analyzing contributor types, temporal patterns, and namespace distribution
-- Full available dataset (2001-2010 range per min/max inspection)

WITH recent_edits AS (
  SELECT
    revision_id,
    title,
    wp_namespace,
    contributor_ip,
    contributor_id,
    contributor_username,
    TIMESTAMP_SECONDS(timestamp) AS edit_timestamp,
    is_minor,
    is_bot,
    num_characters,
    reversion_id,
    comment,
    -- Classify contributor type
    CASE
      WHEN is_bot = TRUE THEN 'Bot'
      WHEN contributor_id IS NOT NULL THEN 'Registered User'
      WHEN contributor_ip IS NOT NULL THEN 'Anonymous (IP)'
      ELSE 'Unknown'
    END AS contributor_type,
    -- Extract date components
    EXTRACT(YEAR FROM TIMESTAMP_SECONDS(timestamp)) AS edit_year,
    EXTRACT(MONTH FROM TIMESTAMP_SECONDS(timestamp)) AS edit_month,
    EXTRACT(DAYOFWEEK FROM TIMESTAMP_SECONDS(timestamp)) AS day_of_week,
    EXTRACT(HOUR FROM TIMESTAMP_SECONDS(timestamp)) AS hour_of_day,
    -- Classify namespace
    CASE
      WHEN wp_namespace = 0 THEN 'Main (Article)'
      WHEN wp_namespace = 1 THEN 'Talk'
      WHEN wp_namespace = 2 THEN 'User'
      WHEN wp_namespace = 3 THEN 'User Talk'
      WHEN wp_namespace IN (4, 5) THEN 'Wikipedia/Project'
      WHEN wp_namespace IN (6, 7) THEN 'File'
      WHEN wp_namespace IN (10, 11) THEN 'Template'
      WHEN wp_namespace IN (14, 15) THEN 'Category'
      ELSE 'Other'
    END AS namespace_category
  FROM
    `Your-Project-ID-Here.samples.wikipedia`
  WHERE
    -- Removed temporal restriction to allow analysis; dataset may predate 2020
    timestamp IS NOT NULL
),

contributor_stats AS (
  SELECT
    contributor_type,
    contributor_id,
    contributor_username,
    COUNT(*) AS total_edits,
    COUNT(DISTINCT title) AS distinct_articles,
    SUM(CASE WHEN is_minor = TRUE THEN 1 ELSE 0 END) AS minor_edits,
    SUM(CASE WHEN reversion_id IS NOT NULL THEN 1 ELSE 0 END) AS reverted_edits,
    AVG(num_characters) AS avg_article_length,
    MIN(edit_timestamp) AS first_edit,
    MAX(edit_timestamp) AS last_edit
  FROM recent_edits
  WHERE contributor_type IN ('Registered User', 'Bot', 'Anonymous (IP)')
  GROUP BY contributor_type, contributor_id, contributor_username
),

TopHuman AS (
  SELECT
    contributor_id,
    /* contributor_username removed from final outputs for privacy */
    total_edits,
    distinct_articles,
    minor_edits,
    reverted_edits,
    avg_article_length,
    first_edit,
    last_edit,
    TIMESTAMP_DIFF(last_edit, first_edit, DAY) AS days_active,
    ROUND(total_edits / NULLIF(TIMESTAMP_DIFF(last_edit, first_edit, DAY), 0), 2) AS edits_per_day,
    ROW_NUMBER() OVER (ORDER BY total_edits DESC) AS contributor_rank
  FROM contributor_stats
  WHERE contributor_type = 'Registered User'
    AND total_edits >= 100
  ORDER BY total_edits DESC
  LIMIT 1000
)

-- Main output: Comprehensive contribution analysis
SELECT
  'CONTRIBUTOR_TYPE_SUMMARY' AS metric_category,
  contributor_type AS dimension_1,
  NULL AS dimension_2,
  COUNT(DISTINCT CASE WHEN contributor_type = 'Registered User' THEN CAST(contributor_id AS STRING)
                      WHEN contributor_type = 'Anonymous (IP)' THEN contributor_ip
                      ELSE NULL END) AS unique_contributors,
  COUNT(*) AS total_edits,
  SUM(CASE WHEN is_minor = TRUE THEN 1 ELSE 0 END) AS minor_edits,
  SUM(CASE WHEN reversion_id IS NOT NULL THEN 1 ELSE 0 END) AS reverted_edits,
  AVG(num_characters) AS avg_article_length,
  NULL AS year_value,
  NULL AS month_value,
  NULL AS namespace_value,
  NULL AS contributor_name,
  NULL AS contributor_rank
FROM recent_edits
GROUP BY contributor_type

UNION ALL

SELECT
  'YEARLY_TREND' AS metric_category,
  contributor_type AS dimension_1,
  CAST(edit_year AS STRING) AS dimension_2,
  COUNT(DISTINCT CASE WHEN contributor_type = 'Registered User' THEN CAST(contributor_id AS STRING)
                      WHEN contributor_type = 'Anonymous (IP)' THEN contributor_ip
                      ELSE NULL END) AS unique_contributors,
  COUNT(*) AS total_edits,
  SUM(CASE WHEN is_minor = TRUE THEN 1 ELSE 0 END) AS minor_edits,
  SUM(CASE WHEN reversion_id IS NOT NULL THEN 1 ELSE 0 END) AS reverted_edits,
  AVG(num_characters) AS avg_article_length,
  edit_year AS year_value,
  NULL AS month_value,
  NULL AS namespace_value,
  NULL AS contributor_name,
  NULL AS contributor_rank
FROM recent_edits
GROUP BY contributor_type, edit_year

UNION ALL

SELECT
  'NAMESPACE_DISTRIBUTION' AS metric_category,
  contributor_type AS dimension_1,
  namespace_category AS dimension_2,
  COUNT(DISTINCT CASE WHEN contributor_type = 'Registered User' THEN CAST(contributor_id AS STRING)
                      WHEN contributor_type = 'Anonymous (IP)' THEN contributor_ip
                      ELSE NULL END) AS unique_contributors,
  COUNT(*) AS total_edits,
  SUM(CASE WHEN is_minor = TRUE THEN 1 ELSE 0 END) AS minor_edits,
  SUM(CASE WHEN reversion_id IS NOT NULL THEN 1 ELSE 0 END) AS reverted_edits,
  AVG(num_characters) AS avg_article_length,
  NULL AS year_value,
  NULL AS month_value,
  wp_namespace AS namespace_value,
  NULL AS contributor_name,
  NULL AS contributor_rank
FROM recent_edits
GROUP BY contributor_type, namespace_category, wp_namespace

UNION ALL

SELECT
  'TOP_HUMAN_CONTRIBUTORS' AS metric_category,
  'Registered User' AS dimension_1,
  CONCAT('rank_', CAST(contributor_rank AS STRING)) AS dimension_2,
  NULL AS unique_contributors,
  total_edits,
  minor_edits,
  reverted_edits,
  avg_article_length,
  NULL AS year_value,
  NULL AS month_value,
  NULL AS namespace_value,
  NULL AS contributor_name,
  contributor_rank
FROM TopHuman

ORDER BY metric_category, dimension_1, total_edits DESC
