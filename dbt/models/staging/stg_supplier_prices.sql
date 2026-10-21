-- One row per row of every supplier file. Cleans SKU and cost; keeps the cost text as it arrived.
with source as (
    select * from {{ source('raw', 'supplier_prices') }}
)

select
    source_file,
    trim(sku)                                as sku,
    cast(valid_from as date)                 as valid_from,
    coalesce(cost_sar, unit_cost)            as raw_cost,      -- column renamed in W39
    try_cast(
        translate(trim(coalesce(cost_sar, unit_cost)), '٠١٢٣٤٥٦٧٨٩٫,', '0123456789..')
        as double
    )                                        as unit_cost_sar  -- NULL when it isn't a number
from source