CREATE TABLE `Your-Project-ID-Here.covid19_geotab_mobility_impact.commercial_traffic`
(
  alpha_code_3 STRING OPTIONS(description="If the record contains country-level stats: 3-letter Country code"),
  country_iso_code_2 STRING OPTIONS(description="If the record contains state-level stats: 2-letter country code, followed by dash, then 2-letter state code."),
  region STRING OPTIONS(description="If the record contains region-level stats: region description as found in `bigquery-public-data.covid19_geotab_mobility_impact.lookup_region`"),
  date DATE OPTIONS(description="Date of the data"),
  day_of_week INT64 OPTIONS(description="Day of Week starting with Sunday as Day 1. Range between [1,7]"),
  percent_of_baseline_activity FLOAT64 OPTIONS(description="Number of trips taken on DATE, relative to the median value for the same day-of-week during baseline period"),
  percent_of_baseline_commercial FLOAT64 OPTIONS(description="percent_of_baseline_activity counting only trips starting around commercial buildings, as labelled by OpenStreetMap"),
  percent_of_baseline_industrial FLOAT64 OPTIONS(description="percent_of_baseline_activity counting only trips starting around industrial buildings, as labelled by OpenStreetMap"),
  percent_of_baseline_warehouse FLOAT64 OPTIONS(description="percent_of_baseline_activity counting only trips starting around warehouse buildings, as labelled by OpenStreetMap"),
  percent_of_baseline_grocery_store FLOAT64 OPTIONS(description="percent_of_baseline_activity counting only trips starting around grocery store buildings, as labelled by OpenStreetMap"),
  percent_of_baseline_other_retail FLOAT64 OPTIONS(description="percent_of_baseline_activity counting only trips starting around all retail buildings, excluding those classified as GroceryStore, as labelled by OpenStreetMap"),
  region_geom GEOGRAPHY OPTIONS(description="Geographic shape of the country, region, or state; for performance, shapes of regions and states are rough approximates of real boundaries."),
  version STRING OPTIONS(description="Version of the table")
)
PARTITION BY date
CLUSTER BY alpha_code_3, country_iso_code_2, day_of_week, region
OPTIONS(
  description="The commercial traffic dataset measures the volume of commercial activity each day (at local time) from March 16 onwards, as measured by number of trips taken. Volume of activity is calculated on a relative basis using data from Feb. 1st and March 15th, 2020 as a benchmark, controlled for day-of-week. Activity is further classified by industry based on the destination location of each trip, using OpenStreetMap's building tag; no classification are made if the trip does not end around a building, though the trip will count towards PercentOfBaselineActivity."
);
