CREATE TABLE `Your-Project-ID-Here.covid19_geotab_mobility_impact.city_congestion`
(
  city_name STRING OPTIONS(description="Name of city"),
  date_time DATETIME OPTIONS(description="Date and time of the measurement"),
  percent_congestion FLOAT64 OPTIONS(description="Percent of congestion relative to the baseline")
)
CLUSTER BY city_name
OPTIONS(
  description="This dataset presents the relative level of congestion in eight select cities on an hourly basis starting February 1st 2020. The goal of this dataset is to provide users with a time series illustrating the effect of the COVID-19 crisis on city congestion. The cities included in the analysis are Atlanta, Chicago, Los Angeles, Mexico City, New York, San Francisco, Seattle and Washington."
);
