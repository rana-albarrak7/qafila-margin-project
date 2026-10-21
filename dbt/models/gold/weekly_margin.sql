-- One row per iso_week x city x category that has at least one counted order line.
with counted_lines as (
    -- rule 7: delivered, quantity > 0, order date on or after the first supplier week
    select
        o.order_date,
        s.city,
        p.category,
        p.sku,
        i.quantity,
        round(i.quantity * i.unit_price - i.discount, 2) as revenue_sar   -- rule 8
    from {{ ref('stg_order_items') }} i
    join {{ ref('stg_orders') }}   o on i.order_id   = o.order_id
    join {{ ref('stg_stores') }}   s on o.store_id   = s.store_id
    join {{ ref('stg_products') }} p on i.product_id = p.product_id
    where o.status = 'delivered'
      and i.quantity > 0
      and o.order_date >= date '2026-08-31'
),

costed_lines as (
    -- point-in-time join: the cost version valid on the order date (valid_to is exclusive).
    -- left join keeps lines with no valid version: their cost is NULL (uncosted).
    select
        l.*,
        l.quantity * h.unit_cost_sar as cost_sar
    from counted_lines l
    left join {{ ref('product_cost_history') }} h
           on h.sku = l.sku
          and h.valid_from <= l.order_date
          and (h.valid_to is null or l.order_date < h.valid_to)
),

aggregated as (
    select
        cast(week(order_date) as integer)                                    as iso_week,  -- ISO week in DuckDB
        city,
        category,
        cast(count(*) as integer)                                            as lines,
        round(sum(revenue_sar), 2)                                           as revenue_sar,
        round(coalesce(sum(revenue_sar) filter (where cost_sar is not null), 0), 2) as costed_revenue_sar,
        round(coalesce(sum(cost_sar), 0), 2)                                 as cost_sar,
        cast(count(*) filter (where cost_sar is null) as integer)            as uncosted_lines
    from costed_lines
    group by 1, 2, 3
)

select
    iso_week,
    city,
    category,
    lines,
    revenue_sar,
    costed_revenue_sar,
    cost_sar,
    round(costed_revenue_sar - cost_sar, 2) as margin_sar,
    uncosted_lines
from aggregated