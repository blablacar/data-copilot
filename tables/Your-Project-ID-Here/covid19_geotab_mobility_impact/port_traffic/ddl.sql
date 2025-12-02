CREATE TABLE `Your-Project-ID-Here.covid19_geotab_mobility_impact.port_traffic`
(
  country_iso_code_2 STRING OPTIONS(description="ISO 3166-2 code representing the county and state/province where the port is located"),
  port_id STRING OPTIONS(description="Unique port identifier"),
  port STRING OPTIONS(description="Port name"),
  year_week STRING OPTIONS(description="Concatenation of the year and week for which the aggregation values are calculated. Format <YYYYWW>"),
  week_end DATE OPTIONS(description="The last day of the week for the aggregation calculations. Format <YYYY-MM-DD>"),
  aggregation_method STRING OPTIONS(description="Define the level of the aggregation. Weekly: the average metric is calculated across the full week prior to the week_end"),
  percent_of_vehicle_volume_change FLOAT64 OPTIONS(description="The percentage change in vehicle volumes in relation to the previous week"),
  percent_of_trip_volume_change FLOAT64 OPTIONS(description="The percentage change in trip volumes in relation to the previous week"),
  percent_of_hdt_volume_change FLOAT64 OPTIONS(description="The percentage change in Hdt vehicle volumes in relation to the previous week. Hdt = heavy duty trucks; weight class 7-8"),
  percent_of_mdt_volume_change FLOAT64 OPTIONS(description="The percentage change in Mdt vehicle volumes in relation to the previous week. Mdt = medium duty trucks; weight class 4-6"),
  percent_of_ldt_volume_change FLOAT64 OPTIONS(description="The percentage change in Ldt vehicle volumes in relation to the previous week. Ldt = light duty trucks; weight class 1-3 | A-H"),
  percent_of_mpv_volume_change FLOAT64 OPTIONS(description="The percentage change in Mpv vehicle volumes in relation to the previous week. Mpv = multi-purpose vehicle"),
  percent_of_car_volume_change FLOAT64 OPTIONS(description="The percentage change in car vehicle volumes in relation to the previous week."),
  percent_of_other_volume_change FLOAT64 OPTIONS(description="The percentage change in vehicle volumes in relation to the previous week, for all vehicles that are not classed as a Hdt, Mdt, Ldt, Mpv or car"),
  percent_of_agg_truck_volume_change FLOAT64 OPTIONS(description="The percentage change in vehicle volumes in relation to the previous week, for a combined grouping of Hdt, Mdt and Ldt vehicle types"),
  percent_of_agg_nontruck_volume_change FLOAT64 OPTIONS(description="The percentage change in vehicle volumes in relation to the previous week, for a combined grouping of Mpv, car and other vehicle types"),
  wait_time_avg FLOAT64 OPTIONS(description="The average wait time (in minutes)"),
  wait_time_hdt FLOAT64 OPTIONS(description="The average wait time (in minutes) for Hdt vehicle types. Hdt = heavy duty trucks; weight class 7-8"),
  wait_time_mdt FLOAT64 OPTIONS(description="The average wait time (in minutes) for Mdt vehicle types. Mdt = medium duty trucks; weight class 4-6"),
  wait_time_ldt FLOAT64 OPTIONS(description="The average wait time (in minutes) for Mdt vehicle types Ldt = light duty trucks; weight class 1-3 | A-H"),
  wait_time_mpv FLOAT64 OPTIONS(description="The average wait time (in minutes) for Mdt vehicle types Mpv = multi-purpose vehicle"),
  wait_time_car FLOAT64 OPTIONS(description="The average wait time (in minutes) for car vehicle types"),
  wait_time_other FLOAT64 OPTIONS(description="The average wait time (in minutes) for all vehicles that are not classed as a Hdt, Mdt, Ldt, Mpv or car"),
  wait_time_agg_truck FLOAT64 OPTIONS(description="The average wait time (in minutes) for a combined grouping of Hdt, Mdt and Ldt vehicle types"),
  wait_time_agg_nontruck FLOAT64 OPTIONS(description="The average wait time (in minutes) for a combined grouping of Mpv, car and other vehicle types"),
  city STRING OPTIONS(description="City in which the port resides"),
  port_latitude FLOAT64 OPTIONS(description="Latitude of the centroid for the port"),
  port_longitude FLOAT64 OPTIONS(description="Longitude of the centroid for the port"),
  version STRING OPTIONS(description="Version of the table"),
  port_geom GEOGRAPHY OPTIONS(description="Polygon object of the port bounds")
)
PARTITION BY week_end
CLUSTER BY country_iso_code_2, city, port_geom, port_id
OPTIONS(
  description="This dataset shows the average percentage week-on-week volume changes and the average weekly wait times for ports. It includes aggregate metrics for the ports as a whole, broken down by each vehicle type and for 2 high-level vehicle type groupings."
);
