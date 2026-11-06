import argparse
from pathlib import Path

from pipelines.ingestion import db

SCHEMA_FILES = (
    "bronze.sql",
    "silver.sql",
    "gold.sql",
    "ops.sql",
)


def schema_directory() -> Path:
    """Lokasi folder database/schemas."""

    return db.PROJECT_ROOT / "database" / "schemas"


def initialize(target: str = "local", verbose: bool = True) -> list[str]:
    """Terapkan semua file schema ke profil database yang dipilih."""

    target = db.ensure_local_target(target)
    applied: list[str] = []
    directory = schema_directory()
    with db.connect(target) as connection:
        for name in SCHEMA_FILES:
            path = directory / name
            if not path.exists():
                raise FileNotFoundError(f"Schema file not found: {path}")
            statement_count = db.run_sql_file(connection, path)
            applied.append(name)
            if verbose:
                print(f"[init] applied {name} ({statement_count} statements)")
    return applied


def main() -> None:
    parser = argparse.ArgumentParser(description="Initialize local PostgreSQL schemas")
    parser.add_argument("--target", default="local", choices=("local",))
    args = parser.parse_args()
    applied = initialize(target=args.target)
    print(f"[init] done: {', '.join(applied)}")


if __name__ == "__main__":
    main()
