CREATE TABLE `Your-Project-ID-Here.covid19_geotab_mobility_impact.airport_traffic`
(
  aggregation_method STRING OPTIONS(description="Aggregation period used to compute this metric"),
  date DATE OPTIONS(description="Date of the data"),
  version STRING OPTIONS(description="Version of the table"),
  airport_name STRING OPTIONS(description="Aggregation period used to compute this metric"),
  percent_of_baseline FLOAT64 OPTIONS(description="Proportion of trips on this date as compared to Avg number of trips on the same day of week in baseline period i.e 1st February 2020 - 15th March 2020"),
  center_point_geom GEOGRAPHY OPTIONS(description="Geographic representation of the centroid of the Airport polygon"),
  city STRING OPTIONS(description="City within which the Airport is located"),
  state_region STRING OPTIONS(description="State within which the Airport is located"),
  country_iso_code_2 STRING OPTIONS(description="ISO 3166-2 code representing the county and subdivision within which the Airport is located"),
  country_name STRING OPTIONS(description="Full text name of the country within which the Airport is located"),
  airport_geom GEOGRAPHY OPTIONS(description="Geographic representation of the Airport polygon")
)
PARTITION BY date
CLUSTER BY airport_name, country_iso_code_2, airport_geom
OPTIONS(
  description="This dataset shows traffic to and from the Airport as a Percentage of the Traffic volume during the baseline period. The baseline period used for computing this metric is from 1st Feb to 15th March 2020. The dataset gets updated daily."
);
