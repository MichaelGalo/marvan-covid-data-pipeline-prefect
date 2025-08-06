{{ config(
  materialized="table",
  unique_key="id"
)

}}

SELECT
    "id",
    "year",
    "month",
    "group",
    "subgroup1",
    "subgroup2",
    "COVID_deaths" AS "covid_deaths",
    "crude_COVID_rate" AS "crude_covid_death_rate",
    "aa_COVID_rate" AS "age_adjusted_covid_death_rate",
    "crude_COVID_rate_ann" AS "annualized_crude_covid_death_rate",
    "aa_COVID_rate_ann" AS "annualized_age_adjusted_covid_death_rate",
    "footnote"
FROM {{ source("RAW", "US_DEATHCOUNTS_RAW") }}
WHERE "jurisdiction_residence" = 'United States'
