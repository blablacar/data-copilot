CREATE TABLE `Your-Project-ID-Here.covid19_geotab_mobility_impact.fuel_station_daily_fillups`
(
  date DATE OPTIONS(description="Date of the data"),
  vehicle_class STRING OPTIONS(description="Vehicle class"),
  percent_of_normal_volume FLOAT64 OPTIONS(description="The percentage of fuelling events at fuel stations relative to the baseline period."),
  version STRING OPTIONS(description="Version of the table")
)
PARTITION BY date
CLUSTER BY vehicle_class, date
OPTIONS(
  description="Percentage of normal number of fuelling events at fuel stations in North America relative to the period immediately pre-Covid-19. Aggregated by day and vehicle class."
);
