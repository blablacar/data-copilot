-- Usage queries for this table
-- Total queries found: 1 (showing top 1)

-- Query 1:
-- --------------------------------------------------

-- COVID-19 Impact on US Port Traffic
-- Analysis of port traffic week-over-week changes
-- Note: Port data shows percentage change from previous week, not baseline comparison

WITH us_port_data AS (
  SELECT
    week_end,
    year_week,
    port,
    city,
    country_iso_code_2,
    percent_of_vehicle_volume_change,
    percent_of_trip_volume_change,
    percent_of_agg_truck_volume_change,
    percent_of_agg_nontruck_volume_change,
    wait_time_avg,
    wait_time_agg_truck,
    wait_time_agg_nontruck,
    EXTRACT(YEAR FROM week_end) AS year,
    EXTRACT(MONTH FROM week_end) AS month
  FROM `Your-Project-ID-Here.covid19_geotab_mobility_impact.port_traffic`
  WHERE
    -- Partition filter (MANDATORY for performance)
    week_end >= '2020-02-01'
    AND week_end <= '2021-12-31'
    -- Focus on US ports (Washington state ports are US-WA)
    AND STARTS_WITH(country_iso_code_2, 'US')
),
monthly_aggregates AS (
  SELECT
    year,
    month,
    port,
    city,
    country_iso_code_2,
    AVG(percent_of_vehicle_volume_change) AS avg_vehicle_change,
    AVG(percent_of_trip_volume_change) AS avg_trip_change,
    AVG(percent_of_agg_truck_volume_change) AS avg_truck_change,
    AVG(percent_of_agg_nontruck_volume_change) AS avg_nontruck_change,
    AVG(wait_time_avg) AS avg_wait_time,
    AVG(wait_time_agg_truck) AS avg_truck_wait_time,
    AVG(wait_time_agg_nontruck) AS avg_nontruck_wait_time,
    MIN(percent_of_vehicle_volume_change) AS min_vehicle_change,
    MAX(percent_of_vehicle_volume_change) AS max_vehicle_change,
    COUNT(*) AS weeks_in_month
  FROM us_port_data
  GROUP BY year, month, port, city, country_iso_code_2
),
port_summary AS (
  SELECT
    port,
    city,
    country_iso_code_2,
    MIN(min_vehicle_change) AS worst_week_decline,
    MAX(max_vehicle_change) AS best_week_growth,
    AVG(avg_vehicle_change) AS overall_avg_change
  FROM monthly_aggregates
  GROUP BY port, city, country_iso_code_2
)
SELECT
    ma.year,
    ma.month,
    ma.port,
    ma.city,
    ma.country_iso_code_2,
    ma.avg_vehicle_change,
    ma.avg_trip_change,
    ma.avg_truck_change,
    ma.avg_nontruck_change,
    ma.avg_wait_time,
    ma.avg_truck_wait_time,
    ma.avg_nontruck_wait_time,
    ma.min_vehicle_change,
    ma.max_vehicle_change,
    ps.worst_week_decline,
    ps.best_week_growth,
    ps.overall_avg_change
FROM monthly_aggregates ma
JOIN port_summary ps
  ON ma.port = ps.port
  AND ma.city = ps.city
  AND ma.country_iso_code_2 = ps.country_iso_code_2
ORDER BY ma.year, ma.month, ma.port

-- ================================================================================
