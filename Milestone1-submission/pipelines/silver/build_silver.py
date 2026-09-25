"""Silver normalization: typed, validated, deduplicated entities."""

import argparse
from pathlib import Path
from typing import Any

from pipelines.ingestion import db

# lokasi file SQL
SQL_FILE = Path(__file__).with_name("build_silver.sql")

SILVER_TABLES = (
    "city_reference",
    "customers",
    "customer_profiles",
    "customer_addresses",
    "products",
    "product_categories",
    "stores",
    "sales_channels",
    "promotions",
    "orders",
    "order_items",
    "order_promotions",
    "payment_events",
    "refund_events",
    "return_events",
    "support_events",
    "web_events",
    "inventory_snapshots",
    "campaign_spend",
    "rejected_records",
)
# Dict comprehension — 20 tabel sekaligus jadi 20 query
COUNTS = {
    f"silver.{table}": f"SELECT COUNT(*) FROM silver.{table}" for table in SILVER_TABLES
}
# REJECTION_REASONS
REJECTION_REASONS = (
    "SELECT reason_code, COUNT(*) FROM silver.rejected_records "
    "GROUP BY reason_code ORDER BY reason_code"
)


def run(
    pipeline_run_id: str, target: str = "local", verbose: bool = True
) -> dict[str, Any]:
    target = db.ensure_local_target(target)

    with db.connect(target) as connection:
        statement_count = db.run_sql_file(
            connection, SQL_FILE, {"pipeline_run_id": pipeline_run_id}
        )
        counts = {name: db.scalar(connection, sql) for name, sql in COUNTS.items()}
        rejections = {
            str(reason): int(total)
            for reason, total in db.fetchall(connection, REJECTION_REASONS)
        }

    if verbose:
        print(f"[silver] executed {statement_count} statements")
        for name, total in counts.items():
            print(f"[silver] {name}: {total}")
        if rejections:
            print(f"[silver] rejected: {rejections}")

    return {
        "pipeline_run_id": pipeline_run_id,
        "statements": statement_count,
        "counts": counts,
        "rejections": rejections,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the Silver layer")
    parser.add_argument("--target", default="local", choices=("local",))
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    run(args.run_id, target=args.target)


if __name__ == "__main__":
    main()
