-- Usage queries for this table
-- Total queries found: 3 (showing top 3)

-- Query 1:
-- --------------------------------------------------

SELECT distinct country_name, country_iso_code_2 FROM `Your-Project-ID-Here.covid19_geotab_mobility_impact.airport_traffic`

-- ================================================================================

-- Query 2:
-- --------------------------------------------------

-- COVID-19 Impact on US Airport Traffic
-- Analysis of airport traffic trends compared to pre-COVID baseline (Feb 1 - March 15, 2020)
-- This query aggregates daily airport traffic data to identify impact patterns and recovery trajectories

WITH us_airport_data AS (
  SELECT
    date,
    airport_name,
    city,
    state_region,
    country_name,
    percent_of_baseline,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(WEEK FROM date) AS week_num
  FROM `Your-Project-ID-Here.covid19_geotab_mobility_impact.airport_traffic`
  WHERE
    -- Partition filter (MANDATORY for performance)
    date >= '2020-02-01'
    AND date <= '2021-12-31'
    -- Focus on US airports
    AND country_name = 'United States'
),
monthly_aggregates AS (
  SELECT
    year,
    month,
    airport_name,
    city,
    state_region,
    AVG(percent_of_baseline) AS avg_percent_of_baseline,
    MIN(percent_of_baseline) AS min_percent_of_baseline,
    MAX(percent_of_baseline) AS max_percent_of_baseline,
    COUNT(*) AS days_in_month
  FROM us_airport_data
  GROUP BY year, month, airport_name, city, state_region
),
airport_summary AS (
  SELECT
    airport_name,
    city,
    state_region,
    MIN(min_percent_of_baseline) AS lowest_traffic_pct,
    MAX(max_percent_of_baseline) AS highest_traffic_pct,
    AVG(avg_percent_of_baseline) AS overall_avg_pct
  FROM monthly_aggregates
  GROUP BY airport_name, city, state_region
)
SELECT
    ma.year,
    ma.month,
    ma.airport_name,
    ma.city,
    ma.state_region,
    ma.avg_percent_of_baseline,
    ma.min_percent_of_baseline,
    ma.max_percent_of_baseline,
    asu.lowest_traffic_pct AS airport_lowest_ever,
    asu.highest_traffic_pct AS airport_highest_ever,
    asu.overall_avg_pct AS airport_overall_avg
FROM monthly_aggregates ma
JOIN airport_summary asu
  ON ma.airport_name = asu.airport_name
  AND ma.city = asu.city
  AND ma.state_region = asu.state_region
ORDER BY ma.year, ma.month, ma.state_region, ma.airport_name

-- ================================================================================

-- Query 3:
-- --------------------------------------------------

-- COVID-19 Impact on US Airport Traffic
-- Analysis of airport traffic trends compared to pre-COVID baseline (Feb 1 - March 15, 2020)
-- This query aggregates daily airport traffic data to identify impact patterns and recovery trajectories

WITH us_airport_data AS (
  SELECT
    date,
    airport_name,
    city,
    state_region,
    country_name,
    percent_of_baseline,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(WEEK FROM date) AS week_num
  FROM `Your-Project-ID-Here.covid19_geotab_mobility_impact.airport_traffic`
  WHERE
    -- Partition filter (MANDATORY for performance)
    date >= '2020-02-01'
    AND date <= '2021-12-31'
    -- Focus on US airports
    AND country_name = 'United States of America (the)'
),
monthly_aggregates AS (
  SELECT
    year,
    month,
    airport_name,
    city,
    state_region,
    AVG(percent_of_baseline) AS avg_percent_of_baseline,
    MIN(percent_of_baseline) AS min_percent_of_baseline,
    MAX(percent_of_baseline) AS max_percent_of_baseline,
    COUNT(*) AS days_in_month
  FROM us_airport_data
  GROUP BY year, month, airport_name, city, state_region
),
airport_summary AS (
  SELECT
    airport_name,
    city,
    state_region,
    MIN(min_percent_of_baseline) AS lowest_traffic_pct,
    MAX(max_percent_of_baseline) AS highest_traffic_pct,
    AVG(avg_percent_of_baseline) AS overall_avg_pct
  FROM monthly_aggregates
  GROUP BY airport_name, city, state_region
)
SELECT
    ma.year,
    ma.month,
    ma.airport_name,
    ma.city,
    ma.state_region,
    ma.avg_percent_of_baseline,
    ma.min_percent_of_baseline,
    ma.max_percent_of_baseline,
    asu.lowest_traffic_pct AS airport_lowest_ever,
    asu.highest_traffic_pct AS airport_highest_ever,
    asu.overall_avg_pct AS airport_overall_avg
FROM monthly_aggregates ma
JOIN airport_summary asu
  ON ma.airport_name = asu.airport_name
  AND ma.city = asu.city
  AND ma.state_region = asu.state_region
ORDER BY ma.year, ma.month, ma.state_region, ma.airport_name

-- ================================================================================
