import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from pipelines.ingestion import db
from pipelines.ingestion.files import (
    EXPECTED_FILES,
    ManifestReport,
    RawFile,
    discover_files,
    iter_records,
    record_identifier,
    validate_manifest,
)

BATCH_SIZE = 1000

# inti idempotensi
INSERT_RAW_RECORDS = """
INSERT INTO bronze.raw_records (
    pipeline_run_id,
    source_family,
    source_file,
    source_row_number,
    source_record_id,
    checksum_sha256,
    payload
) VALUES {values}
ON CONFLICT (source_file, source_row_number) DO UPDATE SET
    pipeline_run_id = EXCLUDED.pipeline_run_id,
    source_family = EXCLUDED.source_family,
    source_record_id = EXCLUDED.source_record_id,
    checksum_sha256 = EXCLUDED.checksum_sha256,
    payload = EXCLUDED.payload,
    ingested_at_utc = now()
"""


# sidik jari per record
def _record_checksum(record: dict[str, Any]) -> str:
    payload = json.dumps(
        record, sort_keys=True, ensure_ascii=False, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# catat run mulai
def _reset_run_metadata(connection: Any, pipeline_run_id: str, raw_root: Path) -> None:
    connection.execute(
        """
        INSERT INTO bronze.ingestion_runs (pipeline_run_id, raw_root)
        VALUES (%s, %s)
        ON CONFLICT (pipeline_run_id) DO UPDATE SET
            raw_root = EXCLUDED.raw_root,
            started_at_utc = now(),
            status = 'running'
        """,
        (pipeline_run_id, str(raw_root)),
    )


#  inti pekerjaan
def load_file(connection: Any, raw_file: RawFile, pipeline_run_id: str) -> int:
    connection.execute(
        "DELETE FROM bronze.raw_records WHERE source_file = %s",
        (raw_file.source_file,),
    )

    rows: list[tuple[Any, ...]] = []
    total = 0
    for row_number, record in iter_records(raw_file.absolute_path):
        fallback = f"{raw_file.source_file}#{row_number}"
        rows.append(
            (
                pipeline_run_id,
                raw_file.family,
                raw_file.source_file,
                row_number,
                record_identifier(record, fallback),
                _record_checksum(record),
                json.dumps(record, ensure_ascii=False),
            )
        )
        if len(rows) >= BATCH_SIZE:
            _insert_batch(connection, rows)
            total += len(rows)
            rows = []
    if rows:
        _insert_batch(connection, rows)
        total += len(rows)

    connection.execute(
        """
        INSERT INTO bronze.source_files (
            source_file, source_family, file_format, checksum_sha256,
            row_count, pipeline_run_id
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (source_file) DO UPDATE SET
            source_family = EXCLUDED.source_family,
            file_format = EXCLUDED.file_format,
            checksum_sha256 = EXCLUDED.checksum_sha256,
            row_count = EXCLUDED.row_count,
            pipeline_run_id = EXCLUDED.pipeline_run_id,
            loaded_at_utc = now()
        """,
        (
            raw_file.source_file,
            raw_file.family,
            raw_file.file_format,
            raw_file.checksum_sha256,
            total,
            pipeline_run_id,
        ),
    )
    return total


# penyusun SQL batch
def _insert_batch(connection: Any, rows: list[tuple[Any, ...]]) -> None:
    placeholders = ",".join(["(%s, %s, %s, %s, %s, %s, %s::jsonb)"] * len(rows))
    parameters: list[Any] = []
    for row in rows:
        parameters.extend(row)
    connection.execute(INSERT_RAW_RECORDS.format(values=placeholders), parameters)


# tandai hasil manifest
def _stamp_manifest_status(
    connection: Any, report: ManifestReport, pipeline_run_id: str
) -> None:
    for raw_file, status in report.manifest_checksums.items():
        checksum_status = (
            "match" if raw_file not in report.checksum_mismatches else "mismatch"
        )
        connection.execute(
            """
            UPDATE bronze.source_files
            SET manifest_checksum = %s, manifest_status = %s
            WHERE source_file = %s
            """,
            (status, checksum_status, raw_file),
        )
    connection.execute(
        "UPDATE bronze.source_files SET manifest_status = 'not_checked' WHERE pipeline_run_id = %s AND manifest_checksum IS NULL",
        (pipeline_run_id,),
    )


# tutup catatan run
def finish_run(
    connection: Any,
    pipeline_run_id: str,
    report: ManifestReport,
    files_found: int,
    status: str = "success",
) -> None:
    connection.execute(
        """
        UPDATE bronze.ingestion_runs
        SET manifest_present = %s,
            manifest_valid = %s,
            files_expected = %s,
            files_found = %s,
            missing_sources = %s,
            status = %s,
            finished_at_utc = now()
        WHERE pipeline_run_id = %s
        """,
        (
            report.manifest_present,
            report.manifest_valid,
            len(EXPECTED_FILES),
            files_found,
            report.missing_sources,
            status,
            pipeline_run_id,
        ),
    )


# orkestrator
def run(
    raw_root: Path,
    pipeline_run_id: str,
    target: str = "local",
    verbose: bool = True,
) -> dict[str, Any]:
    target = db.ensure_local_target(target)
    discovered = discover_files(raw_root)
    report = validate_manifest(raw_root, discovered)

    stats: dict[str, Any] = {
        "pipeline_run_id": pipeline_run_id,
        "raw_root": str(raw_root),
        "files_found": len(discovered),
        "files_expected": len(EXPECTED_FILES),
        "missing_sources": report.missing_sources,
        "manifest_present": report.manifest_present,
        "manifest_valid": report.manifest_valid,
        "checksum_mismatches": report.checksum_mismatches,
        "rows_loaded": {},
    }

    with db.connect(target) as connection:
        _reset_run_metadata(connection, pipeline_run_id, raw_root)
        for raw_file in discovered:
            row_count = load_file(connection, raw_file, pipeline_run_id)
            stats["rows_loaded"][raw_file.source_file] = row_count
            if verbose:
                print(f"[bronze] {raw_file.source_file}: {row_count} rows")
        _stamp_manifest_status(connection, report, pipeline_run_id)
        finish_run(connection, pipeline_run_id, report, len(discovered))

    if verbose and report.missing_sources:
        print(f"[bronze] MISSING SOURCES: {', '.join(report.missing_sources)}")

    return stats


# pintu terminal
def main() -> None:
    from pipelines.run_pipeline import new_pipeline_run_id

    parser = argparse.ArgumentParser(description="Load raw data lake into Bronze")
    parser.add_argument("--input", default="data/raw")
    parser.add_argument("--target", default="local", choices=("local",))
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()

    run_id = args.run_id or new_pipeline_run_id()
    run(Path(args.input), run_id, target=args.target)


if __name__ == "__main__":
    main()
