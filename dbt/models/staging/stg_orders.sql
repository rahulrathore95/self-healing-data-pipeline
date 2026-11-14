-- Staging: the healed warehouse table, minus anything the pipeline quarantined.
with source as (
    select * from {{ source('warehouse', 'orders') }}
),
quarantined as (
    select distinct order_id from {{ source('warehouse', 'orders_quarantine') }}
)
select
    source.order_id,
    source.customer_id,
    cast(source.order_ts as timestamp) as order_ts,
    cast(source.amount as numeric) as amount,
    source.currency,
    source.status
from source
left join quarantined using (order_id)
where quarantined.order_id is null
