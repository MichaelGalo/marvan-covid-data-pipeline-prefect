{{ config(
    materialized="table",
    schema="CLEANED"
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
    PERCENT,
    DATA_QUALITY_RATING,
    CURRENT_TIMESTAMP AS LAST_UPDATED
FROM {{ source("STAGED", "STG_CA_RAPIDTESTDEMAND") }}
