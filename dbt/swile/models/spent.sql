{{ config(materialized='incremental', unique_key='date') }}

with transactions as (
    select date(created_at) as "date", naf_code, amount
    from {{ ref('transactions') }}
    join {{ ref('siret_naf') }}
    using(siret)

    {% if is_incremental() %}

    where created_at > (select max("date") from {{ this }})

    {% endif %}
)

, aggregated_spent as (
    select "date", naf_code, sum(amount) as spent
    from transactions
    group by 1, 2
)

, dates as (
    select distinct date(created_at) as "date"
    from {{ ref('transactions') }}

    {% if is_incremental() %}

    where created_at > (select max("date") from {{ this }})

    {% endif %}
)
, dates_naf as (
    select distinct "date",  naf_code, 0 as spent
    from {{ ref('siret_naf') }}
    cross join dates
)

, spent_with_missing_naf as (
    select *
    from aggregated_spent
    union all
    select *
    from dates_naf
)

, spent_with_missing_naf_aggregated as (
    select "date", naf_code, sum(spent) as "spent"
)
select *
from spent_with_missing_naf_aggregated