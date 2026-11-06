import argparse
import time
import uuid
from pathlib import Path
from typing import Any

from pipelines.bronze import build_bronze
from pipelines.gold import build_gold
from pipelines.ingestion import db, initialize_database
from pipelines.ingestion.files import EXPECTED_FILES, discover_files, validate_manifest
from pipelines.silver import build_silver

DAG_ID = "retail_omnichannel_pipeline"


def new_pipeline_run_id() -> str:
    """Buat id run unik: run-<timestamp UTC>-<8 karakter acak>."""

    timestamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    return f"run-{timestamp}-{uuid.uuid4().hex[:8]}"


def start_run(target: str, pipeline_run_id: str, dag_id: str) -> None:
    """Catat awal run di ops.pipeline_runs."""

    with db.connect(target) as connection:
        connection.execute(
            """
            INSERT INTO ops.pipeline_runs (pipeline_run_id, dag_id, status, env_target)
            VALUES (%s, %s, 'running', %s)
            ON CONFLICT (pipeline_run_id) DO UPDATE SET
                dag_id = EXCLUDED.dag_id,
                status = 'running',
                started_at_utc = now(),
                finished_at_utc = NULL,
                error_message = NULL
            """,
            (pipeline_run_id, dag_id, target),
        )


def record_stage(
    target: str,
    pipeline_run_id: str,
    stage_name: str,
    status: str,
    rows_affected: int | None = None,
    details: dict[str, Any] | None = None,
    duration_ms: int | None = None,
) -> None:
    """Catat hasil satu tahap ke ops.pipeline_stages (best-effort)."""

    import json

    try:
        with db.connect(target) as connection:
            connection.execute(
                """
                INSERT INTO ops.pipeline_stages (
                    pipeline_run_id, stage_name, status, rows_affected, details, duration_ms,
                    finished_at_utc
                ) VALUES (%s, %s, %s, %s, %s::jsonb, %s, now())
                """,
                (
                    pipeline_run_id,
                    stage_name,
                    status,
                    rows_affected,
                    json.dumps(details or {}, default=str),
                    duration_ms,
                ),
            )
            connection.execute(
                "UPDATE ops.pipeline_runs SET current_stage = %s WHERE pipeline_run_id = %s",
                (stage_name, pipeline_run_id),
            )
    except Exception as exc:
        print(f"[ops] warning: could not record stage {stage_name}: {exc}")


def finish_run(
    target: str,
    pipeline_run_id: str,
    status: str,
    metrics: dict[str, Any] | None = None,
    error_message: str | None = None,
) -> None:
    """Tutup run: status akhir, durasi, ringkasan metric, dan pesan error."""

    import json

    with db.connect(target) as connection:
        connection.execute(
            """
            UPDATE ops.pipeline_runs
            SET status = %s,
                finished_at_utc = now(),
                duration_ms = CAST(EXTRACT(EPOCH FROM (now() - started_at_utc)) * 1000 AS BIGINT),
                metrics = %s::jsonb,
                error_message = %s
            WHERE pipeline_run_id = %s
            """,
            (
                status,
                json.dumps(metrics or {}, default=str),
                error_message,
                pipeline_run_id,
            ),
        )


def stage_validate_raw(
    input_dir: Path, target: str, pipeline_run_id: str
) -> dict[str, Any]:
    """Tahap 1: pastikan data lake ada isinya dan laporkan sumber yang hilang."""

    started = time.perf_counter()
    discovered = discover_files(input_dir)
    report = validate_manifest(input_dir, discovered)
    details = {
        "files_found": len(discovered),
        "files_expected": len(EXPECTED_FILES),
        "missing_sources": report.missing_sources,
        "manifest_present": report.manifest_present,
        "manifest_valid": report.manifest_valid,
        "checksum_mismatches": report.checksum_mismatches,
    }
    status = "success" if discovered else "failed"
    record_stage(
        target,
        pipeline_run_id,
        "validate_raw_files",
        status,
        rows_affected=len(discovered),
        details=details,
        duration_ms=int((time.perf_counter() - started) * 1000),
    )
    if status == "failed":
        raise RuntimeError(f"No raw files found under {input_dir}")
    return details


def stage_initialize_schemas(target: str, pipeline_run_id: str) -> dict[str, Any]:
    """Tahap 2: buat keempat schema (bronze, silver, gold, ops) bila belum ada."""

    started = time.perf_counter()
    applied = initialize_database.initialize(target=target, verbose=False)
    record_stage(
        target,
        pipeline_run_id,
        "initialize_schemas",
        "success",
        rows_affected=len(applied),
        details={"schemas": applied},
        duration_ms=int((time.perf_counter() - started) * 1000),
    )
    return {"schemas": applied}


def run_pipeline(
    input_dir: str | Path = "data/raw",
    target: str = "local",
    pipeline_run_id: str | None = None,
    verbose: bool = True,
) -> dict[str, Any]:
    """Jalankan seluruh tahap pipeline dengan quality gate sebelum Gold."""

    target = db.ensure_local_target(target)
    run_id = pipeline_run_id or new_pipeline_run_id()
    input_path = Path(input_dir)
    metrics: dict[str, Any] = {"pipeline_run_id": run_id, "input": str(input_path)}

    initialize_database.initialize(target=target, verbose=False)
    start_run(target, run_id, DAG_ID)

    try:
        metrics["validate_raw_files"] = stage_validate_raw(input_path, target, run_id)
        metrics["initialize_schemas"] = stage_initialize_schemas(target, run_id)

        started = time.perf_counter()
        bronze_stats = build_bronze.run(
            input_path, run_id, target=target, verbose=verbose
        )
        record_stage(
            target,
            run_id,
            "load_bronze",
            "success",
            rows_affected=sum(bronze_stats["rows_loaded"].values()),
            details={
                "missing_sources": bronze_stats["missing_sources"],
                "files_found": bronze_stats["files_found"],
            },
            duration_ms=int((time.perf_counter() - started) * 1000),
        )
        metrics["load_bronze"] = {
            "rows": sum(bronze_stats["rows_loaded"].values()),
            "missing_sources": bronze_stats["missing_sources"],
        }

        started = time.perf_counter()
        silver_stats = build_silver.run(run_id, target=target, verbose=verbose)
        record_stage(
            target,
            run_id,
            "build_silver",
            "success",
            rows_affected=sum(silver_stats["counts"].values()),
            details={
                "counts": silver_stats["counts"],
                "rejections": silver_stats["rejections"],
            },
            duration_ms=int((time.perf_counter() - started) * 1000),
        )
        metrics["build_silver"] = {
            "counts": silver_stats["counts"],
            "rejections": silver_stats["rejections"],
        }

        from pipelines.quality import run_quality_checks

        started = time.perf_counter()
        gate = run_quality_checks.run(
            run_id, target=target, stage="pre_gold", verbose=verbose
        )
        record_stage(
            target,
            run_id,
            "quality_gate_pre_gold",
            "success" if gate["gate_passed"] else "failed",
            rows_affected=len(gate["checks"]),
            details={
                "errors": gate["errors"],
                "warnings_count": len(gate["warnings"]),
            },
            duration_ms=int((time.perf_counter() - started) * 1000),
        )
        metrics["quality_gate_pre_gold"] = {
            "gate_passed": gate["gate_passed"],
            "errors": gate["errors"],
            "warnings": len(gate["warnings"]),
        }
        if not gate["gate_passed"]:
            raise RuntimeError("Pre-Gold quality gate failed; Gold build stopped.")

        started = time.perf_counter()
        gold_stats = build_gold.run(run_id, target=target, verbose=verbose)
        record_stage(
            target,
            run_id,
            "build_gold",
            "success",
            rows_affected=sum(gold_stats["counts"].values()),
            details={
                "counts": gold_stats["counts"],
                "grain": gold_stats["grain_violations"],
            },
            duration_ms=int((time.perf_counter() - started) * 1000),
        )
        metrics["build_gold"] = gold_stats["counts"]

        started = time.perf_counter()
        final_gate = run_quality_checks.run(
            run_id, target=target, stage="post_gold", verbose=verbose
        )
        record_stage(
            target,
            run_id,
            "validate_gold",
            "success" if final_gate["gate_passed"] else "failed",
            rows_affected=len(final_gate["checks"])
            + len(final_gate["reconciliations"]),
            details={
                "errors": final_gate["errors"],
                "failed_reconciliations": [
                    item
                    for item in final_gate["reconciliations"]
                    if item["severity"] == "error" and item["status"] == "fail"
                ],
            },
            duration_ms=int((time.perf_counter() - started) * 1000),
        )
        metrics["validate_gold"] = {
            "gate_passed": final_gate["gate_passed"],
            "warnings": len(final_gate["warnings"]),
        }
        if not final_gate["gate_passed"]:
            raise RuntimeError("Post-Gold quality gate failed.")

        finish_run(target, run_id, "success", metrics=metrics)
        if verbose:
            print(f"[pipeline] SUCCESS run_id={run_id}")
        return metrics

    except Exception as exc:
        finish_run(target, run_id, "failed", metrics=metrics, error_message=str(exc))
        if verbose:
            print(f"[pipeline] FAILED run_id={run_id}: {exc}")
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the retail omnichannel pipeline")
    parser.add_argument("--input", default="data/raw")
    parser.add_argument("--target", default="local", choices=("local",))
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--bronze", default="student", choices=("student",))
    parser.add_argument("--silver", default="student", choices=("student",))
    parser.add_argument("--gold", default="student", choices=("student",))
    args = parser.parse_args()
    run_pipeline(args.input, target=args.target, pipeline_run_id=args.run_id)


if __name__ == "__main__":
    main()
