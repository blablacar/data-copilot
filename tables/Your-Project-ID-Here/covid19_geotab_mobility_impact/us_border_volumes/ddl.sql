CREATE TABLE `Your-Project-ID-Here.covid19_geotab_mobility_impact.us_border_volumes`
(
  trip_direction STRING OPTIONS(description="Direction of the trip"),
  day_type STRING OPTIONS(description="Weekday/Weekend indicator"),
  day_of_week INT64 OPTIONS(description="Day of Week starting with Sunday as Day 1. Range between [1,7]"),
  date DATE OPTIONS(description="Date of the data"),
  avg_crossing_duration FLOAT64 OPTIONS(description="Weighted average crossing duration (min)"),
  percent_of_normal_volume INT64 OPTIONS(description="The overall daily trip volume is compared to avg number of trips in baseline period i.e Jan.1st to Feb.29, 2020."),
  avg_crossing_duration_truck FLOAT64 OPTIONS(description="Weighted average crossing duration (min) for trucks"),
  percent_of_normal_volume_truck INT64 OPTIONS(description="The truck daily trip volume is compared to avg number of trips in baseline period i.e Jan.1st to Feb.29, 2020."),
  version STRING OPTIONS(description="Version of the table")
)
PARTITION BY date
CLUSTER BY trip_direction, day_type, day_of_week, date
OPTIONS(
  description="This dataset is about daily trip volume and weighted average crossing duration per trip direction. This table is updated daily."
);
