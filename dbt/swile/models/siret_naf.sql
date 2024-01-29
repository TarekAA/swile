{{ config(materialized='incremental',
 unique_key='siret',
 post_hook="truncate table {{ source('general', 'siret_naf_staging') }}"
 ) }}

with siret_data as (
    select
        siret,
        naf_code
    from {{ source('general', 'siret_naf_staging') }}
)
select *
from siret_data

{% if is_incremental() %}

{% endif %}