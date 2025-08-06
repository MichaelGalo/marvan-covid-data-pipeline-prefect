{{ config(
    materialized="table",
    unique_key="id"
  )
}}

SELECT
    "id",
    REF_DATE,
    DGUID,
    "North American Industry Classification System (NAICS)",
    "COVID-19 rapid test kits demand and usage",
    VECTOR,
    COORDINATE,
    VALUE as PERCENT,
    CASE
        WHEN STATUS = 'A' THEN 'excellent'
        WHEN STATUS = 'B' THEN 'very good'
        WHEN STATUS = 'C' THEN 'good'
        WHEN STATUS = 'D' THEN 'acceptable'
        WHEN STATUS = 'E' THEN 'use with caution'
        WHEN STATUS = 'F' THEN 'too unreliable to be published'
        WHEN STATUS = '..' THEN 'not available for reference period'
        ELSE STATUS
    END AS DATA_QUALITY_RATING
FROM {{ source("RAW", "CA_RAPIDTESTDEMAND_RAW") }}
WHERE GEO = 'Canada'
