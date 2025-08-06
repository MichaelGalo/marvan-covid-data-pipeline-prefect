{{ config(
    materialized="table",
    unique_key="dataset_id"
  )
}}

SELECT
  dataset_id,
  country,
  dataset_name,
  description,
  last_updated
FROM {{ source("RAW", "DATASET_METADATA_RAW") }}
