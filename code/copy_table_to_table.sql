INSERT INTO usda_data.asd_codes(asd_code, asd_description)
    SELECT cast(asd_code AS INTEGER), asd_desc
    FROM usda_census_crop_totals
    WHERE asd_code <> ' '
    GROUP BY asd_code,
    asd_desc
    ORDER BY asd_code;