-- A SKU can't have two versions starting on the same day (would double-cost order lines).
select sku, valid_from, count(*) as n
from {{ ref('product_cost_history') }}
group by sku, valid_from
having count(*) > 1