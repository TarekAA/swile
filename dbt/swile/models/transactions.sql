{{ config(materialized='incremental',
 unique_key='id',
 post_hook="truncate table {{ source('general', 'transactions_staging') }}") }}

with staged_data as (
    select
        id,
        type,
        amount,
        status,
        created_at,
        wallet_id,
        siret
    from {{ source('general', 'transactions_staging') }}
)
select *
from staged_data

{% if is_incremental() %}

  where created_at > (select max(created_at) from {{ this }})

{% endif %}