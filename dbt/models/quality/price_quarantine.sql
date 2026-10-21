-- One row per supplier row we can't use.
-- Rule: unknown SKU first; otherwise a cost that isn't a positive number.
with prices as (
    select * from {{ ref('stg_supplier_prices') }}
),

products as (
    select sku from {{ ref('stg_products') }}
)

select
    p.source_file,
    p.sku,
    p.valid_from,
    p.raw_cost,
    case
        when pr.sku is null then 'unknown_sku'
        else 'invalid_cost'
    end as reason
from prices p
left join products pr on p.sku = pr.sku
where pr.sku is null
   or p.unit_cost_sar is null
   or p.unit_cost_sar <= 0