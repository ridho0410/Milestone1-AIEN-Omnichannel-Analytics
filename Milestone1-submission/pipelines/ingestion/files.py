import csv
import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

MANIFEST_NAME = "manifest.json"

# Daftar file yang seharusnya ada di data lake beserta nama keluarganya.
# Dipakai untuk mendeteksi sumber yang belum tersedia (missing sources).
EXPECTED_FILES: dict[str, str] = {
    "operational/customers.json": "operational",
    "operational/customer_profiles.json": "operational",
    "operational/customer_addresses.json": "operational",
    "operational/products.json": "operational",
    "operational/product_categories.json": "operational",
    "operational/stores.json": "operational",
    "operational/sales_channels.json": "operational",
    "operational/promotions.json": "operational",
    "operational/orders.json": "operational",
    "operational/order_items.json": "operational",
    "operational/order_promotions.json": "operational",
    "events/payment_events.json": "events",
    "events/refund_events.json": "events",
    "events/return_events.json": "events",
    "events/support_events.json": "events",
    "events/web_events.json": "events",
    "inventory/inventory_snapshots.csv": "inventory",
    "reference/campaign_spend.csv": "reference",
    "reference/city_reference.json": "reference",
}

# Sumber yang wajib ada agar metric Gold benar (payment & refund menentukan
# captured_payment_amount, refunded_amount, dan refund_rate).
CRITICAL_SOURCES = (
    "operational/orders.json",
    "operational/order_items.json",
    "events/payment_events.json",
    "events/refund_events.json",
)


@dataclass(frozen=True)
class RawFile:
    """Metadata satu file mentah yang ditemukan di data lake."""

    relative_path: str  # path relatif terhadap raw root (OS default)
    absolute_path: Path  # path lengkap di disk
    family: str  # operational / events / inventory / reference
    file_format: str  # json / csv
    checksum_sha256: str  # sidik jari isi file

    @property
    def source_file(self) -> str:
        """Path dengan pemisah '/' agar konsisten dipakai sebagai key di database."""

        return self.relative_path.replace("\\", "/")


@dataclass
class ManifestReport:
    """Hasil perbandingan data lake dengan manifest.json dan daftar file wajib."""

    manifest_present: bool = False  # manifest.json ditemukan?
    manifest_valid: bool = False  # semua checksum cocok?
    missing_sources: list[str] = field(
        default_factory=list
    )  # file wajib yang belum ada
    checksum_mismatches: list[str] = field(
        default_factory=list
    )  # file yang isinya berubah
    unexpected_files: list[str] = field(
        default_factory=list
    )  # file di luar daftar wajib
    manifest_checksums: dict[str, str] = field(default_factory=dict)
    start_date: str | None = None  # periode data menurut manifest
    end_date: str | None = None


def sha256_file(path: Path) -> str:
    """Hitung checksum SHA-256 file dengan pembacaan bertahap (hemat memori)."""

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):  # 1 MB per potong
            digest.update(chunk)
    return digest.hexdigest()


def _file_format(path: Path) -> str:
    """Tentukan format file dari ekstensinya."""

    suffix = path.suffix.lower()
    if suffix == ".json":
        return "json"
    if suffix == ".csv":
        return "csv"
    return suffix.lstrip(".") or "unknown"


def discover_files(raw_root: Path) -> list[RawFile]:
    """Telusuri raw_root dan kembalikan metadata semua file (kecuali manifest)."""

    discovered: list[RawFile] = []
    for path in sorted(raw_root.rglob("*")):  # urut agar deterministik
        if not path.is_file():
            continue
        relative = path.relative_to(raw_root).as_posix()
        if relative == MANIFEST_NAME:  # manifest tidak dimuat ke Bronze
            continue
        if relative.startswith("."):  # lewati file tersembunyi
            continue
        family = relative.split("/", 1)[0]  # operational/events/inventory/reference
        discovered.append(
            RawFile(
                relative_path=relative,
                absolute_path=path,
                family=family,
                file_format=_file_format(path),
                checksum_sha256=sha256_file(path),
            )
        )
    return discovered


def read_manifest(raw_root: Path) -> dict[str, Any] | None:
    """Baca manifest.json bila ada."""

    manifest_path = raw_root / MANIFEST_NAME
    if not manifest_path.exists():
        return None
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def validate_manifest(raw_root: Path, discovered: list[RawFile]) -> ManifestReport:
    """Bandingkan file yang ditemukan dengan daftar wajib dan checksum manifest.

    manifest.json bersifat opsional: bila tidak ada, fungsi tetap melaporkan
    sumber yang hilang tanpa menggagalkan pipeline.
    """

    report = ManifestReport()
    found = {item.source_file for item in discovered}
    report.missing_sources = sorted(set(EXPECTED_FILES) - found)  # belum ada di disk
    report.unexpected_files = sorted(
        found - set(EXPECTED_FILES)
    )  # ada tapi tidak diharapkan

    manifest = read_manifest(raw_root)
    if manifest is None:
        return report  # manifest opsional

    report.manifest_present = True
    checksums = manifest.get("files") or {}
    report.manifest_checksums = {
        str(key): str(value) for key, value in checksums.items()
    }
    report.start_date = manifest.get("start_date")
    report.end_date = manifest.get("end_date")

    by_path = {item.source_file: item for item in discovered}
    for relative, expected in report.manifest_checksums.items():
        item = by_path.get(relative)
        if item is None:  # file tercatat di manifest tapi hilang
            if relative not in report.missing_sources:
                report.missing_sources.append(relative)
            continue
        if item.checksum_sha256 != expected:  # isi file berubah dari manifest
            report.checksum_mismatches.append(relative)

    report.manifest_valid = not report.checksum_mismatches
    return report


def iter_records(path: Path) -> Iterator[tuple[int, dict[str, Any]]]:
    """Iterasi isi file menjadi pasangan (nomor_baris, record).

    - JSON : array of object (atau satu object tunggal)
    - CSV  : setiap baris menjadi dict dengan nilai masih berupa teks,
             sehingga Silver yang melakukan cast ke tipe sebenarnya.
    """

    if path.suffix.lower() == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):  # toleransi bila isinya satu object
            payload = [payload]
        for index, record in enumerate(payload, start=1):
            yield index, record if isinstance(record, dict) else {"value": record}
        return

    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for index, row in enumerate(reader, start=1):
            yield index, {str(key): value for key, value in row.items()}


def record_identifier(record: dict[str, Any], fallback: str) -> str:
    """Ambil identifier bisnis dari record sumber untuk kolom source_record_id.

    Urutan prioritas disesuaikan dengan berbagai jenis file (operational, event,
    dan referensi). Bila tidak ada yang cocok, dipakai fallback
    ``<file>#<nomor_baris>`` agar kolom tetap terisi.
    """

    for key in (
        "source_row_id",
        "event_id",
        "order_item_id",
        "order_id",
        "customer_id",
        "product_id",
        "address_id",
        "promotion_id",
        "store_id",
        "channel_id",
        "category_id",
        "city_id",
        "ticket_id",
        "return_id",
    ):
        value = record.get(key)
        if value:
            return str(value)

    # Event menyimpan identifier di dalam objek 'payload'.
    payload = record.get("payload")
    if isinstance(payload, dict):
        for key in ("event_id", "order_id", "ticket_id", "return_id"):
            value = payload.get(key)
            if value:
                return str(value)
    return fallback
