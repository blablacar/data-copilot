CREATE TABLE `Your-Project-ID-Here.covid19_google_mobility.mobility_report`
(
  country_region_code STRING OPTIONS(description="2 letter alpha code for the country/region in which changes are measured relative to the baseline. These values correspond with the ISO 3166-1 alpha-2 codes"),
  country_region STRING OPTIONS(description="The country/region in which changes are measured relative to the baseline"),
  sub_region_1 STRING OPTIONS(description="First geographic sub-region in which the data is aggregated. This varies by country/region to ensure privacy and public health value in consultation with local public health authorities"),
  sub_region_2 STRING OPTIONS(description="Second geographic sub-region in which the data is aggregated. This varies by country/region to ensure privacy and public health value in consultation with local public health authorities"),
  metro_area STRING OPTIONS(description="A specific metro area to measure mobility within a given city/metro area. This varies by country/region to ensure privacy and public health value in consultation with local public health authorities"),
  iso_3166_2_code STRING OPTIONS(description="Unique identifier for the geographic region as defined by ISO Standard 3166-2."),
  census_fips_code STRING OPTIONS(description="Unique identifier for each US county as defined by the US Census Bureau. Maps to county_fips_code in other tables"),
  place_id STRING OPTIONS(description="A textual identifier that uniquely identifies a place in the Google Places database and on Google Maps (details). For example ChIJd_Y0eVIvkIARuQyDN0F1LBA. For details see the following link: https://developers.google.com/places/web-service/place-id"),
  date DATE OPTIONS(description="Changes for a given date as compared to baseline. Baseline is the median value for the corresponding day of the week during the 5-week period Jan 3–Feb 6 2020."),
  retail_and_recreation_percent_change_from_baseline INT64 OPTIONS(description="Mobility trends for places like restaurants cafes shopping centers theme parks museums libraries and movie theaters."),
  grocery_and_pharmacy_percent_change_from_baseline INT64 OPTIONS(description="Mobility trends for places like grocery markets food warehouses farmers markets specialty food shops drug stores and pharmacies."),
  parks_percent_change_from_baseline INT64 OPTIONS(description="Mobility trends for places like local parks national parks public beaches marinas dog parks plazas and public gardens."),
  transit_stations_percent_change_from_baseline INT64 OPTIONS(description="Mobility trends for places like public transport hubs such as subway bus and train stations."),
  workplaces_percent_change_from_baseline INT64 OPTIONS(description="Mobility trends for places of work."),
  residential_percent_change_from_baseline INT64 OPTIONS(description="Mobility trends for places of residence."),
  source_url STRING OPTIONS(description="Source "),
  etl_timestamp TIMESTAMP OPTIONS(description="Load time for this data row")
)
OPTIONS(
  description=" Terms of use By downloading or using the data, you agree to Google's Terms of Service: https://policies.google.com/terms Description This dataset aims to provide insights into what has changed in response to policies aimed at combating COVID-19. It reports movement trends over time by geography, across different categories of places such as retail and recreation, groceries and pharmacies, parks, transit stations, workplaces, and residential. This dataset is intended to help remediate the impact of COVID-19. It shouldn’t be used for medical diagnostic, prognostic, or treatment purposes. It also isn’t intended to be used for guidance on personal travel plans. To learn more about the dataset, the place categories and how we calculate these trends and preserve privacy, read the data documentation: https://www.google.com/covid19/mobility/data_documentation.html "
);
