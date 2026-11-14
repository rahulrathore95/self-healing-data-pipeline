-- Mart: daily revenue by currency, built on the healed staging layer.
select
    cast(order_ts as date) as order_date,
    currency,
    count(*) as order_count,
    sum(amount) as revenue
from {{ ref('stg_orders') }}
where status != 'cancelled'
group by 1, 2
