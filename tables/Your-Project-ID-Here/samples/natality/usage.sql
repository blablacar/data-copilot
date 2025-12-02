-- Usage queries for this table
-- Total queries found: 2 (showing top 2)

-- Query 1:
-- --------------------------------------------------

-- Query to check data availability and get year range
-- This will help us understand what time periods are available for analysis

SELECT
  MIN(year) as min_year,
  MAX(year) as max_year,
  COUNT(*) as total_births,
  COUNT(DISTINCT year) as distinct_years,
  COUNT(DISTINCT CONCAT(CAST(year AS STRING), '-', CAST(month AS STRING))) as distinct_months
FROM `Your-Project-ID-Here.samples.natality`
WHERE year IS NOT NULL;

-- ================================================================================

-- Query 2:
-- --------------------------------------------------

-- Monthly birth analysis across all available years
-- This query aggregates births by year and month to analyze trends and patterns

WITH monthly_births AS (
  SELECT
    year,
    month,
    COUNT(*) as birth_count,
    AVG(weight_pounds) as avg_birth_weight,
    AVG(mother_age) as avg_mother_age,
    AVG(gestation_weeks) as avg_gestation_weeks,
    -- Calculate percentage of different characteristics
    COUNTIF(is_male) / COUNT(*) * 100 as pct_male,
    COUNTIF(mother_married) / NULLIF(COUNTIF(mother_married IS NOT NULL), 0) * 100 as pct_married_mothers,
    COUNTIF(plurality > 1) / COUNT(*) * 100 as pct_multiple_births
  FROM `Your-Project-ID-Here.samples.natality`
  WHERE
    year IS NOT NULL
    AND month IS NOT NULL
    AND year >= 1969  -- Include all available historical data for context
  GROUP BY year, month
),
year_summary AS (
  SELECT
    year,
    SUM(birth_count) as annual_births
  FROM monthly_births
  GROUP BY year
)
SELECT
  mb.year,
  mb.month,
  mb.birth_count,
  ys.annual_births,
  mb.birth_count / ys.annual_births * 100 as pct_of_annual_births,
  mb.avg_birth_weight,
  mb.avg_mother_age,
  mb.avg_gestation_weeks,
  mb.pct_male,
  mb.pct_married_mothers,
  mb.pct_multiple_births,
  -- Calculate month-over-month change
  LAG(mb.birth_count) OVER (ORDER BY mb.year, mb.month) as prev_month_births,
  (mb.birth_count - LAG(mb.birth_count) OVER (ORDER BY mb.year, mb.month)) /
    NULLIF(LAG(mb.birth_count) OVER (ORDER BY mb.year, mb.month), 0) * 100 as mom_pct_change,
  -- Calculate year-over-year change for same month
  LAG(mb.birth_count, 12) OVER (ORDER BY mb.year, mb.month) as prev_year_same_month_births,
  (mb.birth_count - LAG(mb.birth_count, 12) OVER (ORDER BY mb.year, mb.month)) /
    NULLIF(LAG(mb.birth_count, 12) OVER (ORDER BY mb.year, mb.month), 0) * 100 as yoy_pct_change
FROM monthly_births mb
JOIN year_summary ys ON mb.year = ys.year
ORDER BY mb.year, mb.month;

-- ================================================================================
