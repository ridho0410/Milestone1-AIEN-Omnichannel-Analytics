"""Gold analytical build: five contract tables with fixed grain."""

import argparse
from pathlib import Path
from typing import Any

from pipelines.ingestion import db

SQL_FILE = Path(__file__).with_name("build_gold.sql")

GOLD_TABLES = (
    "order_360",
    "customer_daily",
    "product_daily",
    "channel_campaign_daily",
    "executive_kpis_daily",
)

COUNTS = {
    f"gold.{table}": f"SELECT COUNT(*) FROM gold.{table}" for table in GOLD_TABLES
}

GRAIN_CHECKS = {
    "order_360_unique_order": db.grain_sql("gold.order_360", "order_id"),
    "customer_daily_unique_grain": db.grain_sql(
        "gold.customer_daily", "customer_id", "metric_date"
    ),
    "product_daily_unique_grain": db.grain_sql(
        "gold.product_daily", "product_id", "metric_date"
    ),
    "channel_campaign_unique_grain": db.grain_sql(
        "gold.channel_campaign_daily", "metric_date", "sales_channel", "campaign_id"
    ),
    "executive_unique_date": db.grain_sql("gold.executive_kpis_daily", "metric_date"),
}


def run(
    pipeline_run_id: str, target: str = "local", verbose: bool = True
) -> dict[str, Any]:
    target = db.ensure_local_target(target)

    with db.connect(target) as connection:
        statement_count = db.run_sql_file(
            connection, SQL_FILE, {"pipeline_run_id": pipeline_run_id}
        )
        counts = {name: db.scalar(connection, sql) for name, sql in COUNTS.items()}
        grain = {name: db.scalar(connection, sql) for name, sql in GRAIN_CHECKS.items()}

    if verbose:
        print(f"[gold] executed {statement_count} statements")
        for name, total in counts.items():
            print(f"[gold] {name}: {total}")
        for name, duplicates in grain.items():
            flag = "OK" if duplicates == 0 else "DUPLICATE!"
            print(f"[gold] grain {name}: {duplicates} ({flag})")

    return {
        "pipeline_run_id": pipeline_run_id,
        "statements": statement_count,
        "counts": counts,
        "grain_violations": grain,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the Gold layer")
    parser.add_argument("--target", default="local", choices=("local",))
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    run(args.run_id, target=args.target)


if __name__ == "__main__":
    main()
