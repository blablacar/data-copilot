CREATE TABLE `Your-Project-ID-Here.covid19_geotab_mobility_impact.commercial_traffic_by_industry`
(
  alpha_code_3 STRING OPTIONS(description="If the record contains country-level stats: 3-letter Country code"),
  region STRING OPTIONS(description="If the record contains region-level stats: region description as found in `bigquery-public-data.covid19_geotab_mobility_impact.lookup_region`"),
  industry STRING OPTIONS(description="Major industry impacted by COVID."),
  date DATE OPTIONS(description="Date of the data"),
  percent_of_baseline FLOAT64 OPTIONS(description="Percent of baseline activity for the corresponding industry and region."),
  version STRING OPTIONS(description="Version of the table")
)
PARTITION BY date
CLUSTER BY alpha_code_3, industry, region
OPTIONS(
  description="The commercial traffic dataset measures the volume of commercial activity each day (at local time) from March 16 onwards, as measured by number of trips taken. Volume of activity is calculated on a relative basis using data from Feb. 1st and March 15th, 2020 as a benchmark, controlled for day-of-week. Activity is further classified by industry based on the destination location of each trip, using OpenStreetMap's building tag; no classification are made if the trip does not end around a building, though the trip will count towards percent_of_baseline_activity."
);
