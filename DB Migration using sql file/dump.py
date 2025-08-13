import os
import io
import gzip
import sys
import requests
from tqdm import tqdm
from datetime import datetime
import mysql.connector as mysql

# ────────────────────────────────────────────────────────────────────────────────
# SQL Importer for MySQL
# Supports: local .sql / .sql.gz, URL, or raw SQL string
# Handles: DELIMITER changes (procedures/triggers), transactions, FK checks
# ────────────────────────────────────────────────────────────────────────────────


def _open_sql_source(source: str) -> io.TextIOBase:
    """
    Open the SQL source and return a text stream (utf-8).
    - HTTP/HTTPS URL
    - Local .sql or .sql.gz
    - Raw SQL string (heuristic: contains spaces/keywords and not a file path)
    """
    if source.lower().startswith(("http://", "https://")):
        print(f"🌐 Downloading SQL from {source} ...")
        r = requests.get(source, timeout=60)
        r.raise_for_status()
        # auto-handle gzip if server returns gz
        raw = r.content
        if source.endswith(".gz"):
            return io.TextIOWrapper(
                io.BytesIO(gzip.decompress(raw)), encoding="utf-8", newline=""
            )
        return io.StringIO(r.text)

    if os.path.isfile(source):
        print(f"📂 Reading SQL file: {source}")
        if source.endswith(".gz"):
            return io.TextIOWrapper(
                gzip.open(source, "rb"), encoding="utf-8", newline=""
            )
        return open(source, "r", encoding="utf-8", newline="")

    # Fallback: treat as raw SQL text
    print("📝 Using raw SQL string as source")
    return io.StringIO(source)


def _sql_statement_stream(text_stream: io.TextIOBase):
    """
    Generator that yields SQL statements respecting custom DELIMITER directives.
    Handles:
      - DELIMITER changes (e.g., $$, //)
      - Single-line comments (-- ... , # ...)
      - Block comments /* ... */
    """
    current_delim = ";"
    buf = []
    in_block_comment = False
    delim_token = "DELIMITER "

    for raw_line in text_stream:
        line = raw_line

        # Normalize line endings
        if in_block_comment:
            end_idx = line.find("*/")
            if end_idx >= 0:
                line = line[end_idx + 2 :]
                in_block_comment = False
            else:
                continue

        # Strip out block comments that start and end on same line
        while "/*" in line:
            start = line.find("/*")
            end = line.find("*/", start + 2)
            if end == -1:
                # starts block comment; remainder discarded until we find end
                line = line[:start]
                in_block_comment = True
                break
            # remove inline block comment
            line = line[:start] + line[end + 2 :]

        if in_block_comment:
            continue

        # Trim leading/trailing whitespace for control checks
        check = line.strip()

        # Skip empty lines
        if not check:
            continue

        # Handle DELIMITER command
        if check.upper().startswith("DELIMITER "):
            # Flush any partial buffer (shouldn't happen if dump is well-formed)
            if buf:
                partial = "".join(buf).strip()
                if partial:
                    # Yield as-is without needing delimiter
                    yield partial
                buf = []
            current_delim = check[len(delim_token) :].strip()
            # MySQL allows strange delimiters; trust dump
            continue

        # Strip single-line comments
        if check.startswith("--") or check.startswith("#"):
            continue

        # Accumulate
        buf.append(line)

        # If the buffer ends with current_delim (ignoring trailing whitespace), emit
        joined = "".join(buf)
        # Work on a copy to test ending
        test = joined.rstrip()
        if current_delim and test.endswith(current_delim):
            stmt = test[: -len(current_delim)].strip()
            if stmt:
                yield stmt
            buf = []  # reset

    # Flush any remaining statement
    tail = "".join(buf).strip()
    if tail:
        yield tail


def migrate_sql_to_mysql(
    source: str,
    host: str = "localhost",
    user: str = "root",
    password: str = "",
    database: str | None = None,
    port: int = 3306,
    commit_every: int = 200,  # commit after N statements
    stop_on_error: bool = False,  # if False, continue on errors
):
    """
    Import SQL into a MySQL database.

    Args:
        source: URL, file path (.sql / .sql.gz), or raw SQL string
        host, user, password, database, port: MySQL connection info
        commit_every: commit periodically
        stop_on_error: stop on first error or continue
    """
    started = datetime.now()
    print(f"🚀 Starting import at {started:%Y-%m-%d %H:%M:%S}")

    # Connect
    print(f"🔌 Connecting to MySQL at {host}:{port} ...")
    conn = mysql.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        port=port,
        autocommit=False,
        charset="utf8mb4",
        use_pure=True,
    )
    cur = conn.cursor()

    # Safety: disable foreign key checks for duration of import
    print("🧩 Disabling foreign key checks...")
    cur.execute("SET FOREIGN_KEY_CHECKS=0;")
    conn.commit()

    # Some dumps rely on ANSI or NO_BACKSLASH_ESCAPES; you can adjust if needed:
    # cur.execute("SET sql_mode='NO_AUTO_VALUE_ON_ZERO';")

    stream = _open_sql_source(source)
    stmt_iter = _sql_statement_stream(stream)

    applied = 0
    failed = 0
    pbar = tqdm(unit="stmt", desc="Executing", total=None)
    batch = 0

    try:
        for stmt in stmt_iter:
            try:
                cur.execute(stmt)
                applied += 1
                batch += 1
            except mysql.Error as e:
                failed += 1
                print(
                    f"\n❌ Error #{failed} at statement #{applied + failed}: {e}\n─── Statement ───\n{stmt[:1000]}\n───────────────"
                )
                if stop_on_error:
                    raise

            pbar.update(1)
            # periodic commit
            if batch >= commit_every:
                conn.commit()
                batch = 0

        # final commit
        if batch > 0:
            conn.commit()

    finally:
        pbar.close()
        # Re-enable FK checks
        print("🧩 Re-enabling foreign key checks...")
        try:
            cur.execute("SET FOREIGN_KEY_CHECKS=1;")
            conn.commit()
        except Exception:
            pass
        cur.close()
        conn.close()
        stream.close()

    ended = datetime.now()
    print(f"✅ Import DONE at {ended:%Y-%m-%d %H:%M:%S} (took {ended - started}).")
    print(f"📊 Applied: {applied} statements | Failed: {failed}")


if __name__ == "__main__":
    # ─── CONFIGURE YOUR SOURCE AND DESTINATION ─────────────────────────────────
    # SOURCE can be:
    # - HTTPS/HTTP URL to a .sql or .sql.gz file
    # - Path to local .sql or .sql.gz file
    # - Raw SQL string
    SOURCE = r"C:\Users\hp\Desktop\Code\Tools\DB Migration using sql file\guvnl_dev.sql"  # e.g., "https://example.com/db_dump.sql.gz"

    # MySQL connection (adjust as needed)
    MYSQL_CONFIG = {
        "host": "localhost",
        "user": "root",
        "password": "",
        "database": "GUVNL",  # or None, if your dump includes CREATE DATABASE/USE
        "port": 3306,
    }

    # Behaviour
    COMMIT_EVERY = 200
    STOP_ON_ERROR = False

    migrate_sql_to_mysql(
        source=SOURCE,
        commit_every=COMMIT_EVERY,
        stop_on_error=STOP_ON_ERROR,
        **MYSQL_CONFIG,
    )
