-- One row per SKU per cost version (SCD2).
-- valid_to is exclusive (= next version's valid_from) and empty for the current version.
with valid_rows as (
    -- supplier rows that are not quarantined, cost rounded to 2 decimals
    select
        p.source_file,
        p.sku,
        p.valid_from,
        round(p.unit_cost_sar, 2) as unit_cost_sar
    from {{ ref('stg_supplier_prices') }} p
    join {{ ref('stg_products') }} pr on p.sku = pr.sku
    where p.unit_cost_sar > 0
),

latest_file_wins as (
    -- same SKU and valid_from in several files: the file whose name sorts last wins
    select sku, valid_from, unit_cost_sar
    from valid_rows
    qualify row_number() over (partition by sku, valid_from order by source_file desc) = 1
),

changes_only as (
    -- a new version starts only when the cost changes.
    -- weeks where a SKU is missing have no row, so the previous cost simply carries on.
    select sku, valid_from, unit_cost_sar
    from latest_file_wins
    qualify unit_cost_sar is distinct from
            lag(unit_cost_sar) over (partition by sku order by valid_from)
),

versions as (
    select
        sku,
        unit_cost_sar,
        valid_from,
        lead(valid_from) over (partition by sku order by valid_from) as valid_to
    from changes_only
)

select
    sku,
    unit_cost_sar,
    valid_from,
    valid_to,
    valid_to is null as is_current
from versions