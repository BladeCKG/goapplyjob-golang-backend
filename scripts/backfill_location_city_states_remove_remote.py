import os
import sqlite3


def database_path_from_env() -> str:
    raw = os.getenv("DATABASE_URL", "file:page_extract.db?_foreign_keys=on").strip()
    if raw.startswith("file:"):
        tail = raw[5:]
        idx = tail.find("?")
        return tail[:idx] if idx >= 0 else tail
    return raw


def main() -> None:
    db_path = database_path_from_env()
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            UPDATE parsed_jobs
            SET location_city = NULL
            WHERE lower(trim(coalesce(location_city, ''))) = 'remote'
            """
        )
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
