-- COVID-19 Impact on US Commercial Traffic by Industry
-- Analysis of full available period showing drop and recovery patterns

SELECT
  industry,
  date,
  percent_of_baseline,
  EXTRACT(YEAR FROM date) AS year_num,
  EXTRACT(MONTH FROM date) AS month_num
FROM
  `Your-Project-ID-Here.covid19_geotab_mobility_impact.commercial_traffic_by_industry`
WHERE
  alpha_code_3 = 'USA'
  AND date >= '2020-01-01'
  AND date <= '2023-12-31'
  AND region IS NULL  -- Country-level data only
ORDER BY
  industry,
  date
