CREATE TABLE `Your-Project-ID-Here.covid19_geotab_mobility_impact.us_border_wait_times`
(
  border_id STRING OPTIONS(description="Unique ID of the border crossing"),
  port_name STRING OPTIONS(description="Port Name in Canada or Mexico"),
  port_name_us STRING OPTIONS(description="Port Name in the US"),
  trip_direction STRING OPTIONS(description="Direction of the trip"),
  hour_local INT64 OPTIONS(description="Local hour of the data"),
  date_local DATE OPTIONS(description="Local date of the data"),
  day_type STRING OPTIONS(description="Weekday/Weekend indicator"),
  date_utc DATE OPTIONS(description="UTC date of the data"),
  hour_utc INT64 OPTIONS(description="UTC hour of the data"),
  avg_crossing_duration FLOAT64 OPTIONS(description="Average border crossing times (in minutes)"),
  aggregation_method STRING OPTIONS(description="Daily Average: the average is taken for the current LocalHour; Weekly Average: the average is taken for the full week prior to the current LocalDate; Monthly Average: the average is taken for the full month prior to the current LocalDate; Yearly Average: the average is taken for the full year prior to the LocalDate"),
  percent_of_baseline_trip_volume FLOAT64 OPTIONS(description="Proportion of trips in this time interval as compared to Avg number of trips on the same hour of day in baseline period i.e 1st February 2020 - 15th March 2020. Data is only available for daily aggregation level with valid baseline number."),
  border_zone GEOGRAPHY OPTIONS(description="Polygon of the Port in Canada or Mexico"),
  province_code STRING OPTIONS(description="ISO 3166-2 Country-Province code in Canada or Mexico"),
  border_zone_us GEOGRAPHY OPTIONS(description="Polygon of the Port in the US"),
  state_code_us STRING OPTIONS(description="ISO 3166-2 Country-State code for US"),
  border_latitude FLOAT64 OPTIONS(description="Latitude of the border"),
  border_longitude FLOAT64 OPTIONS(description="Longitude of the border"),
  border_geohash STRING OPTIONS(description="Geohash of the Border Station with level of 7"),
  version STRING OPTIONS(description="Version of the table")
)
PARTITION BY date_local
CLUSTER BY trip_direction, day_type, border_zone_us, date_local
OPTIONS(
  description="This dataset shows hourly average border crossing duration for US-Canada and US-Mexico borders starting from 2020-03-16. Hourly trip volume is compared to average trip volume calculated between Feb.1st and Mar.15th, 2020 as a control group in each country."
);
