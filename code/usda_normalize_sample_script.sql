-- I started with a dump of 7+ datasets from the the USDA quick stats. I normalized the data to a degree and removed
-- all columns from the original dump that had no variance in value. This script is evolving script I used to extract
-- data from the dump into individual tables in different schemas
INSERT INTO usda_crop_totals.county
(domain_code, domain_category_id, coefficient_of_variance, value, statistics_code, production_code, year, state_fips_code, county_code)
  SELECT
    look_up_tables.domain_descriptions.id,
    look_up_tables.domain_category_descriptions.id,
    cast("CV (%)" AS FLOAT),
    cast(translate("Value", ',', '') AS FLOAT),
    look_up_tables.statistic_descriptions.statistic_code,
    look_up_tables.production_practice_descriptions.id,
    cast(year AS INTEGER),
    cast(usda_census_crop_totals.state_fips_code AS INTEGER),
    cast(usda_census_crop_totals.county_code AS INTEGER)
  FROM usda_census_crop_totals,
    look_up_tables.domain_category_descriptions,
    look_up_tables.domain_descriptions,
    look_up_tables.statistic_descriptions,
    look_up_tables.production_practice_descriptions,
    look_up_tables.county_descriptions
  WHERE agg_level_desc = 'COUNTY' AND
        domaincat_desc = look_up_tables.domain_category_descriptions.description AND
        domain_desc = look_up_tables.domain_descriptions.description AND
        statisticcat_desc = look_up_tables.statistic_descriptions.statistic_category AND
        short_desc = look_up_tables.statistic_descriptions.statistic_description AND
        prodn_practice_desc = look_up_tables.production_practice_descriptions.description AND
        usda_census_crop_totals.county_code ~ '^[0-9]' AND
        "Value" ~ '^[0-9]' AND
        "CV (%)" ~ '^[0-9]' AND
        cast(usda_census_crop_totals.county_code AS INTEGER) = look_up_tables.county_descriptions.county_code AND
        cast(usda_census_crop_totals.state_fips_code AS INTEGER) = look_up_tables.county_descriptions.state_fips_code
;
