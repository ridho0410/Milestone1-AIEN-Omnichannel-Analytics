import re
from pathlib import Path
from typing import Any, Iterable

# connect()          -> membuka koneksi sesuai profil (local/neon)
# dsn_from_environment() -> menyusun string koneksi tanpa membocorkan kredensial
# apply_sql_file()   -> eksekusi file SQL (dipakai sebagai fallback)
from shared.db import (
    PROJECT_ROOT,
    apply_sql_file,
    connect,
    dsn_from_environment,
)

__all__ = [
    "PROJECT_ROOT",
    "apply_sql_file",
    "connect",
    "dsn_from_environment",
    "ensure_local_target",
    "execute",
    "execute_many",
    "fetchall",
    "grain_sql",
    "scalar",
    "split_sql_statements",
    "run_sql_text",
    "run_sql_file",
]

# Aturan milestone: Neon hanya berisi sample Gold milik instructor untuk demo.
# Pipeline peserta WAJIB berjalan di PostgreSQL lokal dan tidak boleh meng-upload
# Bronze/Silver (beserta schema ops) ke Neon.
LOCAL_TARGET = "local"
LOCAL_ONLY_MESSAGE = (
    "Pipeline peserta hanya boleh menulis ke PostgreSQL lokal "
    "(ANALYTICS_DB_TARGET=local). Bronze, Silver, dan Ops tidak boleh diunggah ke "
    "Neon; Neon hanya berisi sample Gold untuk demo instructor."
)


def ensure_local_target(target: str | None) -> str:
    """Tolak target selain 'local' agar Bronze/Silver tidak pernah masuk Neon.

    Dipakai oleh seluruh tahap pipeline sehingga aturan ini ditegakkan oleh kode
    (bukan hanya oleh dokumentasi/kesepakatan).
    """

    selected = (target or LOCAL_TARGET).lower()
    if selected != LOCAL_TARGET:
        raise ValueError(LOCAL_ONLY_MESSAGE)
    return selected


def execute(connection: Any, sql: str, params: Any = None) -> Any:
    """Jalankan satu statement dan kembalikan cursor-nya."""

    return connection.execute(sql, params)


def fetchall(connection: Any, sql: str, params: Any = None) -> list[Any]:
    """Ambil seluruh baris hasil query."""

    return connection.execute(sql, params).fetchall()


def fetchone(connection: Any, sql: str, params: Any = None) -> Any:
    """Ambil satu baris pertama hasil query."""

    return connection.execute(sql, params).fetchone()


def scalar(connection: Any, sql: str, params: Any = None) -> Any:
    """Ambil satu nilai (kolom pertama baris pertama).

    Dipakai untuk check kualitas: query-nya selalu mengembalikan satu angka,
    misalnya COUNT(*) duplikat.
    """

    row = fetchone(connection, sql, params)
    return None if row is None else row[0]


def grain_sql(table: str, *keys: str) -> str:
    """SQL pemeriksaan grain: hasil 0 berarti kombinasi ``keys`` unik.

    Dipakai builder Gold dan quality gate sekaligus, supaya keduanya memakai
    definisi grain yang sama dan tidak bisa berbeda tafsir.
    """

    kolom = ", ".join(keys)
    return (
        f"SELECT COUNT(*) FROM ("
        f"SELECT {kolom} FROM {table} GROUP BY {kolom} HAVING COUNT(*) > 1"
        f") duplicates"
    )


def execute_many(connection: Any, sql: str, rows: Iterable[tuple[Any, ...]]) -> int:
    """Jalankan satu statement untuk setiap baris parameter.

    Semua eksekusi berada dalam transaksi milik pemanggil, sehingga bila ada
    satu yang gagal, seluruh perubahan bisa dibatalkan.
    """

    count = 0
    for row in rows:
        connection.execute(sql, row)
        count += 1
    return count


_LINE_COMMENT = "--"  # komentar satu baris SQL
_BLOCK_START = "/*"  # awal komentar blok
_BLOCK_END = "*/"  # akhir komentar blok


def split_sql_statements(sql: str) -> list[str]:
    """Pecah skrip SQL menjadi statement terpisah.

    Kenapa perlu: driver pg8000 tidak mengizinkan beberapa statement dalam satu
    panggilan execute. Selain itu, memecah manual lebih aman daripada sekadar
    ``sql.split(";")`` karena titik koma bisa muncul di dalam string literal.

    Penanganan yang didukung:
        - komentar baris  : -- ...
        - komentar blok   : /* ... */
        - string literal  : '...' (termasuk escape '')
        - identifier      : "..." (huruf besar/kecil dijaga)
    """

    statements: list[str] = []  # hasil akhir: daftar statement siap eksekusi
    buffer: list[str] = []  # penampung karakter statement yang sedang dibaca
    index = 0
    length = len(sql)
    in_single = False  # sedang di dalam '...'
    in_double = False  # sedang di dalam "..."
    in_line_comment = False  # sedang di dalam -- ...
    in_block_comment = False  # sedang di dalam /* ... */

    while index < length:
        char = sql[index]
        following = sql[index + 1] if index + 1 < length else ""

        # Di dalam komentar baris: abaikan sampai akhir baris.
        if in_line_comment:
            if char == "\n":
                in_line_comment = False
                buffer.append(char)  # newline dipertahankan agar token tidak menempel
            index += 1
            continue

        # Di dalam komentar blok: abaikan sampai penutup */.
        if in_block_comment:
            if char == "*" and following == "/":
                in_block_comment = False
                index += 2
                continue
            index += 1
            continue

        # Di dalam string literal: titik koma bukan pemisah statement.
        if in_single:
            buffer.append(char)
            if char == "'":
                if following == "'":  # '' adalah escape untuk satu kutip
                    buffer.append(following)
                    index += 2
                    continue
                in_single = False
            index += 1
            continue

        # Di dalam identifier berkutip: sama, titik koma bukan pemisah.
        if in_double:
            buffer.append(char)
            if char == '"':
                in_double = False
            index += 1
            continue

        # Deteksi awal komentar.
        if char == "-" and following == "-":
            in_line_comment = True
            index += 2
            continue

        if char == "/" and following == "*":
            in_block_comment = True
            index += 2
            continue

        # Deteksi awal literal/identifier.
        if char == "'":
            in_single = True
            buffer.append(char)
            index += 1
            continue

        if char == '"':
            in_double = True
            buffer.append(char)
            index += 1
            continue

        # Titik koma di luar konteks = pemisah statement.
        if char == ";":
            statement = "".join(buffer).strip()
            if statement:
                statements.append(statement)
            buffer = []
            index += 1
            continue

        buffer.append(char)
        index += 1

    tail = "".join(buffer).strip()  # statement terakhir tanpa titik koma
    if tail:
        statements.append(tail)
    return statements


# Token yang diganti nilainya, mis. {{pipeline_run_id}}.
_TOKEN_PATTERN = re.compile(r"\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}")
# Bentuk yang sudah dibungkus kutip di file SQL: '{{pipeline_run_id}}'.
_QUOTED_TOKEN_PATTERN = re.compile(r"'(\{\{\s*[a-zA-Z_][a-zA-Z0-9_]*\s*\}\})'")


def _sql_literal(value: Any) -> str:
    """Ubah nilai Python menjadi literal SQL yang aman (kutip di-escape)."""

    if value is None:
        return "NULL"
    text = str(value).replace("'", "''")  # cegah SQL injection lewat tanda kutip
    return f"'{text}'"


def render_sql(sql: str, parameters: dict[str, Any] | None = None) -> str:
    """Ganti token ``{{nama}}`` dengan literal SQL yang aman.

    Dua bentuk didukung:
        {{nama}}     -> nilai tanpa kutip di file SQL
        '{{nama}}'   -> kutip di file SQL ikut diganti, sehingga tidak
                        menghasilkan kutip ganda seperti ''nilai''
    """

    parameters = parameters or {}

    def resolve(name: str) -> str:
        if name not in parameters:
            raise KeyError(f"Missing SQL parameter: {name}")
        return _sql_literal(parameters[name])

    def replace_quoted(match: re.Match[str]) -> str:
        inner = _TOKEN_PATTERN.fullmatch(match.group(1))
        assert inner is not None
        return resolve(inner.group(1))

    sql = _QUOTED_TOKEN_PATTERN.sub(replace_quoted, sql)  # bentuk berkutip dulu
    return _TOKEN_PATTERN.sub(lambda match: resolve(match.group(1)), sql)


def run_sql_text(
    connection: Any, sql: str, parameters: dict[str, Any] | None = None
) -> int:
    """Render token, pecah statement, lalu eksekusi satu per satu.

    Mengembalikan jumlah statement yang dijalankan (dipakai untuk logging).
    """

    rendered = render_sql(sql, parameters)
    statements = split_sql_statements(rendered)
    for statement in statements:
        connection.execute(statement)
    return len(statements)


def run_sql_file(
    connection: Any, path: Path, parameters: dict[str, Any] | None = None
) -> int:
    """Versi run_sql_text untuk file SQL di disk."""

    return run_sql_text(connection, path.read_text(encoding="utf-8"), parameters)
