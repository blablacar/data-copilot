-- Usage queries for this table
-- Total queries found: 6 (showing top 6)

-- Query 1:
-- --------------------------------------------------

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

-- ================================================================================

-- Query 2:
-- --------------------------------------------------

-- Wikipedia Contribution Patterns Overview
-- Analyzing contributor types, temporal patterns, and namespace distribution
-- Focused on recent years (2020-2025) with emphasis on human contributors

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

top_human_contributors AS (
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
SELECT * FROM (
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
  FROM top_human_contributors
) ORDER BY metric_category, dimension_1, total_edits DESC

-- ================================================================================

-- Query 3:
-- --------------------------------------------------

-- Wikipedia Contribution Patterns Overview
-- Analyzing contributor types, temporal patterns, and namespace distribution
-- Focused on recent years (2020-2025) with emphasis on human contributors

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
    -- Focus on recent years (2020-2025)
    timestamp >= UNIX_SECONDS(TIMESTAMP('2020-01-01'))
    AND timestamp <= UNIX_SECONDS(TIMESTAMP('2025-11-16'))
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

top_human_contributors AS (
  SELECT
    contributor_id,
    contributor_username,
    total_edits,
    distinct_articles,
    minor_edits,
    reverted_edits,
    avg_article_length,
    first_edit,
    last_edit,
    TIMESTAMP_DIFF(last_edit, first_edit, DAY) AS days_active,
    ROUND(total_edits / NULLIF(TIMESTAMP_DIFF(last_edit, first_edit, DAY), 0), 2) AS edits_per_day
  FROM contributor_stats
  WHERE contributor_type = 'Registered User'
    AND total_edits >= 100  -- Focus on heavy contributors
  ORDER BY total_edits DESC
  LIMIT 1000
)

-- Main output: Comprehensive contribution analysis
SELECT
  -- Overall statistics by contributor type
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
  NULL AS contributor_name
FROM recent_edits
GROUP BY contributor_type

UNION ALL

-- Temporal patterns by year and contributor type
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
  NULL AS contributor_name
FROM recent_edits
GROUP BY contributor_type, edit_year

UNION ALL

-- Namespace distribution
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
  NULL AS contributor_name
FROM recent_edits
GROUP BY contributor_type, namespace_category, wp_namespace

UNION ALL

-- Top human contributors profile
SELECT
  'TOP_HUMAN_CONTRIBUTORS' AS metric_category,
  'Registered User' AS dimension_1,
  contributor_username AS dimension_2,
  NULL AS unique_contributors,
  total_edits,
  minor_edits,
  reverted_edits,
  avg_article_length,
  NULL AS year_value,
  NULL AS month_value,
  NULL AS namespace_value,
  contributor_username AS contributor_name
FROM top_human_contributors

ORDER BY metric_category, dimension_1, total_edits DESC

-- ================================================================================

-- Query 4:
-- --------------------------------------------------

-- Wikipedia Contribution Patterns Overview
-- Analyzing contributor types, temporal patterns, and namespace distribution
-- Focused on recent years (2020-2025) with emphasis on human contributors

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
    -- Focus on recent years (2020-2025)
    timestamp >= UNIX_SECONDS(TIMESTAMP('2020-01-01'))
    AND timestamp <= UNIX_SECONDS(TIMESTAMP('2025-11-16'))
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

top_human_contributors AS (
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
SELECT * FROM (
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
  FROM top_human_contributors
) ORDER BY metric_category, dimension_1, total_edits DESC

-- ================================================================================

-- Query 5:
-- --------------------------------------------------

-- Inspect min/max timestamps and corresponding years in the wikipedia table
SELECT
  MIN(TIMESTAMP_SECONDS(timestamp)) AS min_edit_ts,
  MAX(TIMESTAMP_SECONDS(timestamp)) AS max_edit_ts,
  EXTRACT(YEAR FROM MIN(TIMESTAMP_SECONDS(timestamp))) AS min_year,
  EXTRACT(YEAR FROM MAX(TIMESTAMP_SECONDS(timestamp))) AS max_year,
  COUNT(*) AS total_rows
FROM `Your-Project-ID-Here.samples.wikipedia`;

-- ================================================================================

-- Query 6:
-- --------------------------------------------------

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

-- ================================================================================
