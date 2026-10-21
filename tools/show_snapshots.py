"""Print every snapshot of gold.weekly_margin and the totals it holds (Iceberg time travel).

    uv run python tools/show_snapshots.py [warehouse]
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from run import get_catalog  # noqa: E402

warehouse = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("warehouse")
table = get_catalog(warehouse).load_table("gold.weekly_margin")

for snap in table.snapshots():
    rows = table.scan(snapshot_id=snap.snapshot_id).to_arrow()  # the table as it was at this snapshot
    op = snap.summary.operation.value if snap.summary else "?"
    cost = sum(v for v in rows.column("cost_sar").to_pylist() if v is not None)
    margin = sum(v for v in rows.column("margin_sar").to_pylist() if v is not None)
    print(f"{snap.snapshot_id}  {op:<9} rows={rows.num_rows:<4} cost_sar={cost:,.2f}  margin_sar={margin:,.2f}")