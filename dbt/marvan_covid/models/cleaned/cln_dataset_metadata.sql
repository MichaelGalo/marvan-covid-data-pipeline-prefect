{{ config(
    materialized="table",
    unique_key="DATASET_ID",
    schema="CLEANED"
  )
}}

SELECT
  dataset_id,
  country,
  dataset_name,
  description,
  last_updated
FROM {{ source("STAGED", "STG_DATASET_METADATA") }}
