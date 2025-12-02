CREATE TABLE `Your-Project-ID-Here.covid19_geotab_mobility_impact.fuel_station_weekly_fillups`
(
  week_start DATE OPTIONS(description="The start of the week in which fuelling events at fuel stations was measured."),
  week_end DATE OPTIONS(description="The end of the week in which fuelling events at fuel stations was measured."),
  state_province STRING OPTIONS(description="The state where fuelling events was measured."),
  country_iso_code_2 STRING OPTIONS(description="ISO 3166-2 code representing the county and state/province of the aggregation"),
  percent_of_normal_volume FLOAT64 OPTIONS(description="The percentage of fuelling events at fuel stations relative to the baseline period."),
  version STRING OPTIONS(description="Version of the table")
)
PARTITION BY week_start
CLUSTER BY state_province, week_start, week_end, country_iso_code_2
OPTIONS(
  description="Percentage of normal number of fuelling events at fuel stations in North America relative to the period immediately pre-Covid-19. Aggregated by day and vehicle class."
);
