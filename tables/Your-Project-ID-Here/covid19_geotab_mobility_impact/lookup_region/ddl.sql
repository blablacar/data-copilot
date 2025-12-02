CREATE TABLE `Your-Project-ID-Here.covid19_geotab_mobility_impact.lookup_region`
(
  country_iso_code_2 STRING OPTIONS(description="ISO 3166-2 code representing the county and state/province"),
  region_id STRING OPTIONS(description="Unique identifier for each region"),
  region_description STRING OPTIONS(description="Name of the region"),
  states STRING OPTIONS(description="Concatenated list of states that are contained within the region. Values are separated by a semi-colon (;)")
)
CLUSTER BY country_iso_code_2, region_description, region_id, states
OPTIONS(
  description="A reference table to group states in the US and provinces in Canada into regions; using official federal regions from the Office of Management and Budget for the regions in the US."
);
