-- One row per product.
select
    product_id,
    trim(sku)  as sku,
    category
from {{ source('raw', 'products') }}