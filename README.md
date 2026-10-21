# Qafila Gross Margin

Submitted by: Rana Albarrak — Academy: @SDAIAAcademy

The task requirements, rules, and grading criteria are available in [BRIEF.md](BRIEF.md).

```bash
uv sync
uv run python tools/make_data.py          # generate data/
uv run python run.py                      # data/ -> warehouse/ (DuckDB + Iceberg)
uv run python tools/check.py              # run the contract self-check
```

## Design

The pipeline follows this flow:

`suppliers/*.csv` + `qafila_oltp.sqlite` → **raw** (Python + pandas → DuckDB) → **staging** (dbt views) → **silver / quality** (dbt tables) → **gold** (dbt table) → **Iceberg** (pyiceberg, updated in place)

| Model                                                         | Layer   | Grain (one row per ...)                                   | Materialization             |
| ------------------------------------------------------------- | ------- | --------------------------------------------------------- | --------------------------- |
| `stg_supplier_prices`                                         | staging | row from a supplier price file                            | view                        |
| `stg_orders`, `stg_order_items`, `stg_products`, `stg_stores` | staging | order / order line / product / store                      | view                        |
| `product_cost_history`                                        | silver  | SKU per cost version                                      | table, published to Iceberg |
| `weekly_margin`                                               | gold    | iso_week × city × category with at least one counted line | table, published to Iceberg |
| `price_quarantine`                                            | quality | rejected supplier row                                     | table, published to Iceberg |

### Building the SCD2 history

To build the cost history, I first keep only supplier rows that are not quarantined and round the cost to two decimal places.

If the same SKU and `valid_from` date appear in more than one file, I keep the row from the file whose name sorts last. I do this using `row_number` ordered by `source_file desc`. This means that a correction file such as `prices_2026-W39_fix.csv` replaces the original `prices_2026-W39.csv` values for the same SKU and date.

After that, I use `lag` to compare each cost with the previous cost for the same SKU. I only create a new version when the cost actually changes. The `valid_to` value is then taken from the next version's `valid_from` using `lead`.

If a SKU is missing from one week's supplier file, I do not create a new history row for it. Its previous cost remains valid until another cost version appears.

### Matching order lines to their costs

Each order line is matched to the cost that was valid on its order date.

I use a `LEFT JOIN` with the following time condition:

`valid_from <= order_date < valid_to`

For the current version, where `valid_to` is empty, the cost remains valid for later dates.

I used a `LEFT JOIN` intentionally so that an order line is not removed when no matching cost exists. In that case, the line keeps a NULL cost and is counted as uncosted instead.

## Data Problems I Found

The following counts are from the course dataset using seed 2026.

| Problem                                                         | Where                     | Rows affected | What I did                                                                              |
| --------------------------------------------------------------- | ------------------------- | ------------: | --------------------------------------------------------------------------------------- |
| Byte-order mark at the start of the file                        | `prices_2026-W38.csv`     |        1 file | Read the files using `utf-8-sig`                                                        |
| Decimal comma (`9,49`)                                          | W38                       |           126 | Used `translate` to convert `,` to `.` in staging                                       |
| Arabic-Indic digits and decimal sign (`٨٫٦٥`)                   | W38                       |            34 | Translated `٠-٩` to `0-9` and `٫` to `.`                                                |
| `unit_cost` renamed to `cost_sar`, with a new `currency` column | W39                       |           160 | Unioned files by column name in raw and used `coalesce(cost_sar, unit_cost)` in staging |
| Zero or negative cost                                           | W39                       |             4 | Quarantined as `invalid_cost`                                                           |
| SKU that Qafila does not sell                                   | W39                       |             3 | Quarantined as `unknown_sku`                                                            |
| Trailing spaces in SKU                                          | `products`                |             2 | Used `trim` on both supplier and product SKUs                                           |
| Zero or negative quantity                                       | `order_items`             |           142 | Excluded these lines from margin calculations using `quantity > 0`                      |
| Late correction file                                            | `prices_2026-W39_fix.csv` |             9 | Made the last file name win for the same SKU and `valid_from`                           |

## Tests

I added 16 dbt tests, and all of them pass.

| Test                                        | On                     | What it checks                                                                                                 |
| ------------------------------------------- | ---------------------- | -------------------------------------------------------------------------------------------------------------- |
| `unique`, `not_null` on `sku`               | `stg_products`         | Makes sure trimming does not create duplicate SKUs that could cause duplicate joins                            |
| `assert_history_one_row_per_sku_valid_from` | `product_cost_history` | Makes sure there are not two cost versions for the same SKU starting on the same date                          |
| `assert_one_current_version_per_sku`        | `product_cost_history` | Makes sure each SKU has only one current cost version                                                          |
| `relationships` `sku` → `stg_products`      | `product_cost_history` | Makes sure unknown SKUs never get into the cost history                                                        |
| `accepted_values` on `reason`               | `price_quarantine`     | Makes sure quarantine reasons are only `unknown_sku` or `invalid_cost`                                         |
| `assert_margin_adds_up`                     | `weekly_margin`        | Checks that `margin_sar = costed_revenue_sar - cost_sar` and that costed revenue does not exceed total revenue |
| `assert_weekly_margin_one_row_per_grain`    | `weekly_margin`        | Makes sure there are no duplicate iso_week × city × category rows                                              |

## Re-runs and the Correction

To make re-runs idempotent, `publish()` compares each model with the current Iceberg table before writing anything.

If the rows have not changed, it does not call `overwrite`. Running the pipeline a second time on the same data therefore prints `unchanged, no new snapshot` for all three published tables.

I also tested how the pipeline handles a late correction. After running:

```bash
uv run python tools/make_data.py --add-correction
```

and running the pipeline again, `product_cost_history` increased from 633 to 637 versions, and `gold.weekly_margin` received a new snapshot.

`tools/show_snapshots.py` shows the history of the snapshots:

```text
4542807457167173187  append    rows=165  cost_sar=1,654,199.98  margin_sar=801,016.88   <- before the correction
1884252993982007490  delete    rows=0    cost_sar=0.00          margin_sar=0.00         <- first half of overwrite
4591605851509802595  append    rows=165  cost_sar=1,651,701.06  margin_sar=803,515.80   <- after the correction
```

The important part is that the original snapshot is still available with the pre-correction values, while the newest snapshot contains the corrected results.

To make sure the solution was not dependent only on the course seed, I also tested the complete sequence using `--seed 11`: initial run, re-run, adding the correction, and running again. `tools/check.py` passed all 22/22 checks.

## What I'd Do Next

### If the data became 100× larger

The main thing I would change is the ingestion process. Right now, `ingest()` loads the data into pandas in memory. With much larger data, I would let DuckDB read the CSV files and SQLite data directly instead.

`same_rows()` also compares complete tables. At a larger scale, I would make `weekly_margin` incremental so that only the weeks affected by new or corrected costs need to be rebuilt.

### If supplier files arrived daily

The current logic would still work because the newest file name wins when there are duplicate SKU and `valid_from` values.

However, daily files would create Iceberg snapshots more frequently. I would therefore add regular snapshot expiration and file compaction to prevent unnecessary storage and metadata growth.

## AI Assistance

I used Claude (Anthropic) throughout the task to help me understand the brief, clarify technical concepts such as SCD2, point-in-time joins, and Iceberg snapshots, and work through the implementation with me step by step, including ingest(), the dbt models and tests, publish(), and tools/show_snapshots.py. It was especially helpful when I ran into errors or needed help understanding the code, and it also helped me draft this README. I ran and tested everything myself and verified the final results using tools/check.py on both the course dataset and seed 11.

I ran and tested the pipeline myself, reviewed the results, and verified the final implementation using `tools/check.py` on both the course dataset and seed 11.