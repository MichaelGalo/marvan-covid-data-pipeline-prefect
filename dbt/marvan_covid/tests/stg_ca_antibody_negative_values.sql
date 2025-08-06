{{  config(
  materialized="test")
}}

SELECT
    REF_DATE,
    DGUID,
    "Measure",
    "Sex at birth",
    "Age group",
    "Characteristics",
    VECTOR,
    COORDINATE,
    VALUE AS PERCENT,
    CASE
        WHEN STATUS = 'x' THEN 'suppressed to meet the confidentiality requirements of the Statistics Act'
        WHEN STATUS = 'E' THEN 'use with caution'
        ELSE STATUS
    END AS DATA_QUALITY_RATING
FROM {{ source("RAW", "CA_ANTIBODY_RAW")}}
WHERE GEO = 'Canada' 
  AND VALUE < 0