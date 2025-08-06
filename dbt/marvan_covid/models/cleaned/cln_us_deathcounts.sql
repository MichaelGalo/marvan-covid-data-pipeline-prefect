{{ config(
  materialized="table",
  schema="CLEANED"
)

}}

SELECT
    "id",
    "year",
    "month",
    "group",
    "subgroup1",
    "subgroup2",
    "covid_deaths",
    "crude_covid_death_rate",
    "age_adjusted_covid_death_rate",
    "annualized_crude_covid_death_rate",
    "annualized_age_adjusted_covid_death_rate",
    "footnote",
    CURRENT_TIMESTAMP AS LAST_UPDATED
FROM {{ source("STAGED", "STG_US_DEATHCOUNTS") }}
