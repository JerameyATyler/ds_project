-- This script contains the code to create the views used for easy querying of the data.

DROP VIEW views.pricing_indexes;
CREATE OR REPLACE VIEW views.pricing_indexes AS
  SELECT
    tups.id,
    food_item_descriptions.food_item_description,
    tups.year,
    tups.percent_change,
    tups.pricing_index
  FROM
    (
      (
        SELECT
          *,
          'CONSUMER' AS pricing_index
        FROM
          pricing_indexes."cpi_1974-2016"
      )
      UNION
      (
        SELECT
          *,
          'PRODUCER' AS pricing_index
        FROM
          pricing_indexes."ppi_1974-2016"
      )
    ) AS tups,
    look_up_tables.food_item_descriptions
  WHERE
    tups.food_item_code = food_item_descriptions.food_item_code
  ORDER BY
    food_item_description,
    year;

DROP VIEW views.petroleum_prices;
CREATE OR REPLACE VIEW views.petroleum_prices AS
  SELECT *
  FROM
    (
      (
        SELECT
          *,
          'CRUDE' AS petroleum_state,
          NULL    AS location
        FROM
          petroleum_prices.crude_oil_price_daily
      )
      UNION
      (
        SELECT
          *,
          'REFINED'     AS petroleum_state,
          'GULF_SHORES' AS location
        FROM
          petroleum_prices.refined_oil_price_daily_gulf_coast
      )
      UNION
      (
        SELECT
          *,
          'REFINED'   AS petroleum_state,
          'NY_HARBOR' AS location
        FROM
          petroleum_prices.refined_oil_price_daily_ny
      )
    ) AS tups
  ORDER BY
    day,
    petroleum_state
;

DROP VIEW views.crop_totals_national;
CREATE OR REPLACE VIEW views.crop_totals_national AS
  SELECT
    tups.id,
    tups.statistic_description,
    tups.production_practice_description,
    nd.naics_description,
    tups.year,
    tups.coefficient_of_variance,
    tups.value
  FROM

      (
        SELECT
          national.id,
          domain_descriptions.description,
          statistic_descriptions.statistic_description,
          production_practice_descriptions.description AS production_practice_description,
          CASE
            WHEN substring(domain_category_descriptions.description FROM '(\d+)') ~ '\d+'
            THEN cast(substring(domain_category_descriptions.description FROM '(\d+)') AS INTEGER)
          ELSE
            NULL
          END AS naics_code,
          national.year,
          national.coefficient_of_variance,
          national.value
        FROM
          usda_crop_totals.national,
          look_up_tables.domain_category_descriptions,
          look_up_tables.statistic_descriptions,
          look_up_tables.production_practice_descriptions,
          look_up_tables.domain_descriptions
        WHERE
          national.domain_category_id = domain_category_descriptions.id AND
          national.statistics_code = statistic_descriptions.statistic_code AND
          national.production_code = production_practice_descriptions.id AND
          national.domain_code = domain_descriptions.id AND
          national.statistics_code = 7
      ) as tups
  FULL JOIN
    look_up_tables.naics_descriptions AS nd on nd.naics_code = tups.naics_code
  WHERE
    tups.id is NOT NULL
  ORDER BY
    statistic_description,
    production_practice_description,
    year
  ;

DROP VIEW views.crop_totals_state;
CREATE OR REPLACE VIEW views.crop_totals_state AS
  SELECT
    tups.id,
    tups.state_alpha,
    tups.statistic_description,
    tups.production_practice_description,
    nd.naics_description,
    tups.year,
    tups.coefficient_of_variance,
    tups.value
  FROM

      (
        SELECT
          state.id,
          state_description.state_alpha,
          domain_descriptions.description,
          statistic_descriptions.statistic_description,
          production_practice_descriptions.description AS production_practice_description,
          CASE
            WHEN substring(domain_category_descriptions.description FROM '(\d+)') ~ '\d+'
            THEN cast(substring(domain_category_descriptions.description FROM '(\d+)') AS INTEGER)
          ELSE
            NULL
          END AS naics_code,
          state.year,
          state.coefficient_of_variance,
          state.value
        FROM
          usda_crop_totals.state,
          look_up_tables.domain_category_descriptions,
          look_up_tables.statistic_descriptions,
          look_up_tables.production_practice_descriptions,
          look_up_tables.state_description,
          look_up_tables.domain_descriptions
        WHERE
          state.domain_category_id = domain_category_descriptions.id AND
          state.statistics_code = statistic_descriptions.statistic_code AND
          state.production_code = production_practice_descriptions.id AND
          state.state_fips_code = state_description.state_fips_code AND
          state.domain_code = domain_descriptions.id AND
          state.statistics_code = 7
      ) as tups
  FULL JOIN
    look_up_tables.naics_descriptions AS nd on nd.naics_code = tups.naics_code
  WHERE
    tups.id is NOT NULL
  ORDER BY
    state_alpha,
    statistic_description,
    production_practice_description,
    year
  ;

DROP VIEW views.crop_totals_county;
CREATE OR REPLACE VIEW views.crop_totals_county AS
  SELECT
    tups.id,
    tups.state_alpha,
    tups.county_description,
    tups.latitude,
    tups.longitude,
    tups.statistic_description,
    tups.production_practice_description,
    nd.naics_description,
    tups.year,
    tups.coefficient_of_variance,
    tups.value
  FROM

      (
        SELECT
          county.id,
          state_description.state_alpha,
          county_descriptions.county_description,
          county_descriptions.latitude,
          county_descriptions.longitude,
          domain_descriptions.description,
          statistic_descriptions.statistic_description,
          production_practice_descriptions.description AS production_practice_description,
          CASE
            WHEN substring(domain_category_descriptions.description FROM '(\d+)') ~ '\d+'
            THEN cast(substring(domain_category_descriptions.description FROM '(\d+)') AS INTEGER)
          ELSE
            NULL
          END AS naics_code,
          county.year,
          county.coefficient_of_variance,
          county.value
        FROM
          usda_crop_totals.county,
          look_up_tables.domain_category_descriptions,
          look_up_tables.statistic_descriptions,
          look_up_tables.production_practice_descriptions,
          look_up_tables.state_description,
          look_up_tables.county_descriptions,
          look_up_tables.domain_descriptions
        WHERE
          county.domain_category_id = domain_category_descriptions.id AND
          county.statistics_code = statistic_descriptions.statistic_code AND
          county.production_code = production_practice_descriptions.id AND
          county.state_fips_code = state_description.state_fips_code AND
          county.domain_code = domain_descriptions.id AND
          county.county_code = county_descriptions.county_code AND
          county.statistics_code = 7
      ) as tups
  FULL JOIN
    look_up_tables.naics_descriptions AS nd on nd.naics_code = tups.naics_code
  WHERE
    tups.id is NOT NULL
  ORDER BY
    state_alpha,
    county_description,
    statistic_description,
    production_practice_description,
    year
  ;