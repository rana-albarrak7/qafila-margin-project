-- One row per order line.
select
    order_id,
    line_no,
    product_id,
    quantity,
    unit_price,
    discount
from {{ source('raw', 'order_items') }}