{{ config(materialized='incremental',
 unique_key='siret'
 ) }}

with siret_data as (
    select
        distinct siret
    from {{ ref('transactions') }}

    {% if is_incremental() %}

    where created_at = (select max(created_at::DATE) from {{ ref('transactions') }})

    {% endif %}
)

, missing_siret_naf_data as (
    select distinct siret
    from siret_data
    left join {{ ref('siret_naf') }}
    using (siret)
    where naf_code is null
)

select *
from missing_siret_naf_data

