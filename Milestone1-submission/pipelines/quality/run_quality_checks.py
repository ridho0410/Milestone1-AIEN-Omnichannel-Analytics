import argparse
from dataclasses import dataclass
from typing import Any

from pipelines.ingestion import db

TABLE_EXISTS = "SELECT to_regclass(%s) IS NOT NULL"


@dataclass(frozen=True)
class Check:
    name: str
    layer: str
    sql: str
    expected: float = 0.0
    comparator: str = "eq"
    severity: str = "error"
    detail: str = ""
    requires_table: str | None = None
    requires_rows: str | None = None


@dataclass(frozen=True)
class Reconciliation:
    metric_name: str
    left_source: str
    right_source: str
    left_sql: str
    right_sql: str
    tolerance: float = 0.0
    severity: str = "error"
    requires_rows: str | None = None


QUALITY_CHECKS: tuple[Check, ...] = (
    Check(
        "bronze.rows_present",
        "bronze",
        "SELECT COUNT(*) FROM bronze.raw_records",
        expected=1,
        comparator="ge",
        detail="Bronze harus berisi source evidence.",
    ),
    Check(
        "bronze.missing_critical_sources",
        "bronze",
        "SELECT COUNT(*) FROM (VALUES"
        " ('operational/orders.json'),"
        " ('operational/order_items.json'),"
        " ('events/payment_events.json'),"
        " ('events/refund_events.json')"
        ") AS expected(source_file)"
        " WHERE source_file NOT IN (SELECT source_file FROM bronze.source_files)",
        severity="warning",
        detail="Sumber kritis untuk metric finance belum tersedia di data lake.",
    ),
    Check(
        "silver.orders_unique_order_id",
        "silver",
        db.grain_sql("silver.orders", "order_id"),
        detail="Deduplikasi order harus menyisakan tepat satu baris per order_id.",
    ),
    Check(
        "silver.order_items_unique_item_id",
        "silver",
        db.grain_sql("silver.order_items", "order_item_id"),
    ),
    Check(
        "silver.order_items_no_negative_quantity",
        "silver",
        "SELECT COUNT(*) FROM silver.order_items WHERE quantity <= 0",
        detail="Quantity non-positif harus ditolak ke silver.rejected_records.",
    ),
    Check(
        "silver.order_items_no_broken_product_ref",
        "silver",
        "SELECT COUNT(*) FROM silver.order_items oi"
        " WHERE NOT EXISTS ("
        "   SELECT 1 FROM silver.products p WHERE p.product_id = oi.product_id"
        " )",
    ),
    Check(
        "silver.order_items_no_broken_order_ref",
        "silver",
        "SELECT COUNT(*) FROM silver.order_items oi"
        " WHERE NOT EXISTS ("
        "   SELECT 1 FROM silver.orders o WHERE o.order_id = oi.order_id"
        " )",
    ),
    Check(
        "silver.orders_no_broken_customer_ref",
        "silver",
        "SELECT COUNT(*) FROM silver.orders o"
        " WHERE NOT EXISTS ("
        "   SELECT 1 FROM silver.customers c WHERE c.customer_id = o.customer_id"
        " )",
    ),
    Check(
        "silver.orders_no_missing_channel_ref",
        "silver",
        "SELECT COUNT(*) FROM silver.orders o"
        " WHERE NOT EXISTS ("
        "   SELECT 1 FROM silver.sales_channels c WHERE c.channel_id = o.sales_channel"
        " )",
    ),
    Check(
        "silver.rejected_records_documented",
        "silver",
        "SELECT CASE WHEN COUNT(*) = 0 THEN 0 ELSE 1 END"
        " FROM silver.rejected_records",
        expected=1,
        severity="warning",
        detail="Rejected records harus terisi bila ada record invalid.",
    ),
    Check("gold.order_360_grain", "gold", db.grain_sql("gold.order_360", "order_id")),
    Check(
        "gold.customer_daily_grain",
        "gold",
        db.grain_sql("gold.customer_daily", "customer_id", "metric_date"),
    ),
    Check(
        "gold.product_daily_grain",
        "gold",
        db.grain_sql("gold.product_daily", "product_id", "metric_date"),
    ),
    Check(
        "gold.channel_campaign_daily_grain",
        "gold",
        db.grain_sql(
            "gold.channel_campaign_daily", "metric_date", "sales_channel", "campaign_id"
        ),
    ),
    Check(
        "gold.executive_kpis_daily_grain",
        "gold",
        db.grain_sql("gold.executive_kpis_daily", "metric_date"),
    ),
    Check(
        "gold.order_360_net_revenue_formula",
        "gold",
        "SELECT COUNT(*) FROM gold.order_360"
        " WHERE net_revenue <> ("
        "   gross_merchandise_value - discount_amount + shipping_revenue - refunded_amount"
        " )",
        detail="net_revenue = gross - discount + shipping - refunded.",
    ),
    Check(
        "gold.order_360_non_negative_amounts",
        "gold",
        "SELECT COUNT(*) FROM gold.order_360"
        " WHERE gross_merchandise_value < 0 OR discount_amount < 0"
        "    OR shipping_revenue < 0 OR refunded_amount < 0",
    ),
    Check(
        "gold.product_daily_stockout_consistency",
        "gold",
        "SELECT COUNT(*) FROM gold.product_daily"
        " WHERE stockout_flag IS TRUE AND available_inventory <> 0",
        detail="stockout_flag harus konsisten dengan available_inventory = 0.",
    ),
    Check(
        "gold.executive_kpis_daily_present",
        "gold",
        "SELECT COUNT(*) FROM gold.executive_kpis_daily",
        expected=1,
        comparator="ge",
    ),
)


RECONCILIATIONS: tuple[Reconciliation, ...] = (
    Reconciliation(
        "gross_merchandise_value",
        "gold.order_360",
        "silver.order_items",
        "SELECT COALESCE(SUM(gross_merchandise_value), 0) FROM gold.order_360",
        "SELECT COALESCE(SUM(line_gross), 0) FROM silver.order_items",
        tolerance=0.01,
    ),
    Reconciliation(
        "order_item_count",
        "gold.order_360",
        "silver.order_items",
        "SELECT COALESCE(SUM(item_count), 0) FROM gold.order_360",
        "SELECT COUNT(*) FROM silver.order_items",
    ),
    Reconciliation(
        "order_unit_quantity",
        "gold.order_360",
        "silver.order_items",
        "SELECT COALESCE(SUM(unit_quantity), 0) FROM gold.order_360",
        "SELECT COALESCE(SUM(quantity), 0) FROM silver.order_items",
    ),
    Reconciliation(
        "net_revenue_total",
        "gold.order_360",
        "gold.executive_kpis_daily",
        "SELECT COALESCE(SUM(net_revenue), 0) FROM gold.order_360",
        "SELECT COALESCE(SUM(net_revenue), 0) FROM gold.executive_kpis_daily",
        tolerance=0.01,
    ),
    Reconciliation(
        "order_count_total",
        "gold.order_360",
        "gold.executive_kpis_daily",
        "SELECT COUNT(*) FROM gold.order_360",
        "SELECT COALESCE(SUM(total_orders), 0) FROM gold.executive_kpis_daily",
    ),
    Reconciliation(
        "customer_daily_net_revenue",
        "gold.order_360",
        "gold.customer_daily",
        "SELECT COALESCE(SUM(net_revenue), 0) FROM gold.order_360",
        "SELECT COALESCE(SUM(net_revenue), 0) FROM gold.customer_daily",
        tolerance=0.01,
    ),
    Reconciliation(
        "product_daily_gross_revenue",
        "gold.order_360",
        "gold.product_daily",
        "SELECT COALESCE(SUM(gross_merchandise_value), 0) FROM gold.order_360",
        "SELECT COALESCE(SUM(gross_revenue), 0) FROM gold.product_daily",
        tolerance=0.01,
    ),
    Reconciliation(
        "campaign_attributed_orders_no_multiplication",
        "gold.channel_campaign_daily",
        "gold.order_360",
        "SELECT COALESCE(SUM(attributed_orders), 0) FROM ("
        "  SELECT metric_date, sales_channel, SUM(attributed_orders) AS attributed_orders"
        "  FROM gold.channel_campaign_daily GROUP BY 1, 2"
        ") per_channel_day",
        "SELECT COUNT(*) FROM gold.order_360 o"
        " WHERE EXISTS ("
        "   SELECT 1 FROM gold.channel_campaign_daily c"
        "   WHERE c.metric_date = o.order_date AND c.sales_channel = o.sales_channel"
        " )",
    ),
    Reconciliation(
        "refund_amount_vs_refund_events",
        "gold.order_360",
        "silver.refund_events",
        "SELECT COALESCE(SUM(refunded_amount), 0) FROM gold.order_360",
        "SELECT COALESCE(SUM(amount), 0) FROM silver.refund_events"
        " WHERE UPPER(COALESCE(refund_status, '')) IN"
        " ('COMPLETED','SETTLED','REFUNDED','CLOSED','SUCCESS')",
        tolerance=0.01,
        severity="warning",
        requires_rows="SELECT COUNT(*) FROM silver.refund_events",
    ),
    Reconciliation(
        "captured_payment_vs_payment_events",
        "gold.order_360",
        "silver.payment_events",
        "SELECT COALESCE(SUM(captured_payment_amount), 0) FROM gold.order_360",
        "SELECT COALESCE(SUM(amount), 0) FROM silver.payment_events"
        " WHERE UPPER(COALESCE(payment_status, '')) IN"
        " ('CAPTURED','SETTLED','PAID','SUCCESS')",
        tolerance=0.01,
        severity="warning",
        requires_rows="SELECT COUNT(*) FROM silver.payment_events",
    ),
    Reconciliation(
        "return_status_vs_return_events",
        "gold.order_360",
        "silver.return_events",
        "SELECT COUNT(*) FROM gold.order_360 WHERE return_status <> 'none'",
        "SELECT COUNT(DISTINCT order_id) FROM silver.return_events",
    ),
)


def _compare(observed: float | None, expected: float, comparator: str) -> str:
    if observed is None:
        return "skipped"
    if comparator == "eq":
        return "pass" if abs(observed - expected) <= 1e-6 else "fail"
    if comparator == "ge":
        return "pass" if observed >= expected else "fail"
    if comparator == "le":
        return "pass" if observed <= expected else "fail"
    raise ValueError(f"Unknown comparator: {comparator}")


def _table_ready(connection: Any, table: str | None) -> bool:
    if not table:
        return True
    return bool(db.scalar(connection, TABLE_EXISTS, (table,)))


def _rows_available(connection: Any, sql: str | None) -> bool:
    if not sql:
        return True
    value = db.scalar(connection, sql)
    return bool(value and float(value) > 0)


def run(
    pipeline_run_id: str,
    target: str = "local",
    stage: str = "post_gold",
    verbose: bool = True,
) -> dict[str, Any]:
    if stage not in {"pre_gold", "post_gold"}:
        raise ValueError("stage must be 'pre_gold' or 'post_gold'")

    target = db.ensure_local_target(target)

    allowed_layers = (
        {"bronze", "silver"} if stage == "pre_gold" else {"bronze", "silver", "gold"}
    )
    checks = tuple(check for check in QUALITY_CHECKS if check.layer in allowed_layers)
    reconciliations = RECONCILIATIONS if stage == "post_gold" else ()

    results: list[dict[str, Any]] = []
    reconciliations_results: list[dict[str, Any]] = []

    with db.connect(target) as connection:
        for check in checks:
            if not _table_ready(connection, check.requires_table):
                status = "skipped"
                observed: float | None = None
            elif not _rows_available(connection, check.requires_rows):
                status = "skipped"
                observed = None
            else:
                observed = db.scalar(connection, check.sql)
                observed = None if observed is None else float(observed)
                status = _compare(observed, check.expected, check.comparator)

            connection.execute(
                """
                INSERT INTO ops.quality_checks (
                    pipeline_run_id, check_name, layer, severity, status,
                    observed_value, expected_value, details
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    pipeline_run_id,
                    check.name,
                    check.layer,
                    check.severity,
                    status,
                    observed,
                    check.expected,
                    check.detail or None,
                ),
            )
            results.append(
                {
                    "check_name": check.name,
                    "layer": check.layer,
                    "severity": check.severity,
                    "status": status,
                    "observed": observed,
                    "expected": check.expected,
                }
            )

        for item in reconciliations:
            if not _rows_available(connection, item.requires_rows):
                status = "skipped"
                left = right = variance = None
            else:
                left_value = db.scalar(connection, item.left_sql)
                right_value = db.scalar(connection, item.right_sql)
                left = None if left_value is None else float(left_value)
                right = None if right_value is None else float(right_value)
                variance = None if left is None or right is None else left - right
                status = (
                    "pass"
                    if variance is not None and abs(variance) <= item.tolerance
                    else "fail"
                )

            connection.execute(
                """
                INSERT INTO ops.reconciliation_results (
                    pipeline_run_id, metric_name, left_source, right_source,
                    left_value, right_value, variance, tolerance, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    pipeline_run_id,
                    item.metric_name,
                    item.left_source,
                    item.right_source,
                    left,
                    right,
                    variance,
                    item.tolerance,
                    status,
                ),
            )
            reconciliations_results.append(
                {
                    "metric_name": item.metric_name,
                    "left": left,
                    "right": right,
                    "variance": variance,
                    "tolerance": item.tolerance,
                    "severity": item.severity,
                    "status": status,
                }
            )

    failed_errors = [
        item
        for item in results
        if item["severity"] == "error" and item["status"] == "fail"
    ]
    failed_reconciliations = [
        item
        for item in reconciliations_results
        if item["severity"] == "error" and item["status"] == "fail"
    ]
    warnings = [
        item
        for item in results
        if item["severity"] == "warning" and item["status"] == "fail"
    ] + [
        item
        for item in reconciliations_results
        if item["severity"] == "warning" and item["status"] == "fail"
    ]

    gate_passed = not failed_errors and not failed_reconciliations

    if verbose:
        print(f"[quality:{stage}] checks:")
        for item in results:
            print(
                f"  - {item['check_name']}: {item['status']}"
                f" (observed={item['observed']}, expected={item['expected']})"
            )
        print(f"[quality:{stage}] reconciliations:")
        for item in reconciliations_results:
            print(
                f"  - {item['metric_name']}: {item['status']}"
                f" (variance={item['variance']}, tolerance={item['tolerance']})"
            )
        if warnings:
            print(f"[quality:{stage}] WARNINGS: {len(warnings)}")
        print(f"[quality:{stage}] gate_passed={gate_passed}")

    return {
        "pipeline_run_id": pipeline_run_id,
        "stage": stage,
        "gate_passed": gate_passed,
        "checks": results,
        "reconciliations": reconciliations_results,
        "errors": failed_errors,
        "warnings": warnings,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run pipeline quality checks")
    parser.add_argument("--target", default="local", choices=("local",))
    parser.add_argument(
        "--stage", default="post_gold", choices=("pre_gold", "post_gold")
    )
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    summary = run(args.run_id, target=args.target, stage=args.stage)
    if not summary["gate_passed"]:
        raise SystemExit("Quality gate FAILED")


if __name__ == "__main__":
    main()
