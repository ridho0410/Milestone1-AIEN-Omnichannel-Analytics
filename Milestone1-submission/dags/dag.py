import os
from datetime import datetime, timedelta
from pathlib import Path

from pipelines.bronze import build_bronze
from pipelines.gold import build_gold
from pipelines.ingestion import initialize_database
from pipelines.quality import run_quality_checks
from pipelines.run_pipeline import (
    finish_run,
    record_stage,
    stage_initialize_schemas,
    stage_validate_raw,
    start_run,
)
from pipelines.silver import build_silver

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError as exc:
    DAG = None  # type: ignore[assignment]
    PythonOperator = None  # type: ignore[assignment]
    _AIRFLOW_IMPORT_ERROR = exc
else:
    _AIRFLOW_IMPORT_ERROR = None


DAG_ID = "retail_omnichannel_pipeline"


def _on_task_failure(context: dict) -> None:
    """Tandai run sebagai failed di ops.pipeline_runs saat ada task gagal."""

    run_id = _run_id(context)
    task_id = (
        context.get("task_instance").task_id
        if context.get("task_instance")
        else "unknown"
    )
    exception = context.get("exception")
    message = (
        f"task '{task_id}' failed: {exception}"
        if exception
        else f"task '{task_id}' failed"
    )
    try:
        finish_run("local", run_id, "failed", error_message=message)
    except Exception as exc:
        print(f"[ops] warning: gagal menandai run failed: {exc}")


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "depends_on_past": False,
    "on_failure_callback": _on_task_failure,
}


def _raw_dir() -> Path:
    """Folder data lake yang dibaca; bisa dioverride lewat env."""

    configured = os.getenv("RETAIL_RAW_DIR", "data/raw")
    return Path(configured)


def _target() -> str:
    """Profil database pipeline: selalu lokal."""
    return "local"


def _run_id(context: dict) -> str:
    """Ambil id run dari Airflow agar satu DAG run = satu pipeline_run_id."""

    dag_run = context.get("dag_run")
    if dag_run is not None and getattr(dag_run, "run_id", None):
        return f"airflow-{dag_run.run_id}"
    return "airflow-manual"


def task_validate_raw(**context) -> dict:
    """Tahap 1: validasi file mentah, sekaligus daftarkan run ke ops.pipeline_runs."""

    run_id = _run_id(context)
    start_run("local", run_id, DAG_ID)
    return stage_validate_raw(_raw_dir(), _target(), run_id)


def task_initialize_schemas(**context) -> dict:
    """Tahap 2: buat schema bronze/silver/gold/ops bila belum ada."""

    return stage_initialize_schemas(_target(), _run_id(context))


def task_load_bronze(**context) -> dict:
    """Tahap 3: muat seluruh data lake ke Bronze (payload asli, idempotent)."""

    import time

    run_id = _run_id(context)
    started = time.perf_counter()
    stats = build_bronze.run(_raw_dir(), run_id, target=_target(), verbose=True)
    record_stage(
        _target(),
        run_id,
        "load_bronze",
        "success",
        rows_affected=sum(stats["rows_loaded"].values()),
        details={"missing_sources": stats["missing_sources"]},
        duration_ms=int((time.perf_counter() - started) * 1000),
    )
    return stats


def task_build_silver(**context) -> dict:
    """Tahap 4: normalisasi + validasi + deduplikasi ke Silver."""

    import time

    run_id = _run_id(context)
    started = time.perf_counter()
    stats = build_silver.run(run_id, target=_target(), verbose=True)
    record_stage(
        _target(),
        run_id,
        "build_silver",
        "success",
        rows_affected=sum(stats["counts"].values()),
        details={"counts": stats["counts"], "rejections": stats["rejections"]},
        duration_ms=int((time.perf_counter() - started) * 1000),
    )
    return stats


def task_quality_gate_pre_gold(**context) -> dict:
    """Tahap 5: quality gate sebelum Gold."""

    run_id = _run_id(context)
    summary = run_quality_checks.run(
        run_id, target=_target(), stage="pre_gold", verbose=True
    )
    record_stage(
        _target(),
        run_id,
        "quality_gate_pre_gold",
        "success" if summary["gate_passed"] else "failed",
        rows_affected=len(summary["checks"]),
        details={
            "errors": summary["errors"],
            "warnings_count": len(summary["warnings"]),
        },
    )
    if not summary["gate_passed"]:
        raise ValueError("Pre-Gold quality gate failed")
    return {"gate_passed": True, "warnings": len(summary["warnings"])}


def task_build_gold(**context) -> dict:
    """Tahap 6: bangun lima tabel kontrak Gold (grain + metric sesuai schema)."""

    import time

    run_id = _run_id(context)
    started = time.perf_counter()
    stats = build_gold.run(run_id, target=_target(), verbose=True)
    record_stage(
        _target(),
        run_id,
        "build_gold",
        "success",
        rows_affected=sum(stats["counts"].values()),
        details={"counts": stats["counts"], "grain": stats["grain_violations"]},
        duration_ms=int((time.perf_counter() - started) * 1000),
    )
    return stats


def task_validate_gold(**context) -> dict:
    """Tahap 7: validasi akhir Gold (grain + reconciliation) dan catat metadata."""

    run_id = _run_id(context)
    summary = run_quality_checks.run(
        run_id, target=_target(), stage="post_gold", verbose=True
    )
    failed = [
        item
        for item in summary["reconciliations"]
        if item["severity"] == "error" and item["status"] == "fail"
    ]
    record_stage(
        _target(),
        run_id,
        "validate_gold",
        "success" if summary["gate_passed"] else "failed",
        rows_affected=len(summary["checks"]) + len(summary["reconciliations"]),
        details={"errors": summary["errors"], "failed_reconciliations": failed},
    )
    if not summary["gate_passed"]:
        raise ValueError("Post-Gold quality gate failed")

    finish_run(
        "local",
        run_id,
        "success",
        metrics={
            "checks": len(summary["checks"]),
            "reconciliations": len(summary["reconciliations"]),
            "warnings": len(summary["warnings"]),
        },
    )
    return {"gate_passed": True, "warnings": len(summary["warnings"])}


def build_dag() -> "DAG":
    """Susun DAG beserta urutan dependensinya."""

    if DAG is None:
        raise RuntimeError(
            "Apache Airflow tidak terpasang pada environment ini."
        ) from _AIRFLOW_IMPORT_ERROR

    dag_kwargs = {
        "dag_id": DAG_ID,
        "default_args": DEFAULT_ARGS,
        "start_date": datetime(2026, 1, 1),
        "catchup": False,
        "max_active_runs": 1,
        "tags": ["retail", "milestone", "nl2sql"],
        "description": "Bronze -> Silver -> quality gate -> Gold -> validate",
    }
    try:
        dag = DAG(
            **dag_kwargs, schedule="*/5 * * * *"
        )  # atur jadwal DAG untuk berjalan setiap berapa menit
    except TypeError:
        dag = DAG(**dag_kwargs, schedule_interval=None)

    with dag:
        validate_raw = PythonOperator(
            task_id="validate_raw_files", python_callable=task_validate_raw
        )
        initialize = PythonOperator(
            task_id="initialize_schemas", python_callable=task_initialize_schemas
        )
        bronze = PythonOperator(task_id="load_bronze", python_callable=task_load_bronze)
        silver = PythonOperator(
            task_id="build_silver", python_callable=task_build_silver
        )
        gate = PythonOperator(
            task_id="quality_gate_pre_gold", python_callable=task_quality_gate_pre_gold
        )
        gold = PythonOperator(task_id="build_gold", python_callable=task_build_gold)
        validate = PythonOperator(
            task_id="validate_gold", python_callable=task_validate_gold
        )

        validate_raw >> initialize >> bronze >> silver >> gate >> gold >> validate

    return dag


if DAG is not None:  # pragma: no cover - hanya aktif di environment Airflow
    # Airflow menemukan DAG dari objek module-level bernama `dag`.
    dag = build_dag()
