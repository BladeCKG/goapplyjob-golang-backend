import argparse
import os
import re
import sqlite3


def infer_seniority_flags(role_title: str | None) -> tuple[bool, bool, bool, bool, bool]:
    normalized = re.sub(r"[^a-z0-9]+", " ", (role_title or "").lower()).strip()
    tokens = set(normalized.split())
    is_entry = "entry" in tokens or "intern" in tokens
    is_junior = "junior" in tokens or "jr" in tokens
    is_senior = "senior" in tokens or "sr" in tokens
    is_lead = any(token in tokens for token in ("lead", "principal", "staff", "head"))
    is_mid = not (is_entry or is_junior or is_senior or is_lead)
    return is_entry, is_junior, is_mid, is_senior, is_lead


def database_path_from_env() -> str:
    raw = os.getenv("DATABASE_URL", "file:page_extract.db?_foreign_keys=on").strip()
    if raw.startswith("file:"):
        tail = raw[5:]
        q_idx = tail.find("?")
        return tail[:q_idx] if q_idx >= 0 else tail
    return raw


def run(sources: set[str], dry_run: bool, batch_size: int) -> None:
    db_path = database_path_from_env()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        placeholders = ",".join(["?"] * len(sources))
        rows = conn.execute(
            f"""
            SELECT p.id, p.role_title, p.is_entry_level, p.is_junior, p.is_mid_level, p.is_senior, p.is_lead
            FROM parsed_jobs p
            JOIN raw_us_jobs r ON r.id = p.raw_us_job_id
            WHERE r.source IN ({placeholders})
            ORDER BY p.id ASC
            """,
            tuple(sorted(sources)),
        ).fetchall()

        scanned = 0
        updated = 0
        for row in rows:
            scanned += 1
            is_entry, is_junior, is_mid, is_senior, is_lead = infer_seniority_flags(row["role_title"])
            current = (
                bool(row["is_entry_level"]),
                bool(row["is_junior"]),
                bool(row["is_mid_level"]),
                bool(row["is_senior"]),
                bool(row["is_lead"]),
            )
            next_values = (is_entry, is_junior, is_mid, is_senior, is_lead)
            if current == next_values:
                continue

            updated += 1
            if dry_run:
                continue
            conn.execute(
                """
                UPDATE parsed_jobs
                SET is_entry_level = ?, is_junior = ?, is_mid_level = ?, is_senior = ?, is_lead = ?
                WHERE id = ?
                """,
                (int(is_entry), int(is_junior), int(is_mid), int(is_senior), int(is_lead), row["id"]),
            )
            if updated % max(batch_size, 1) == 0:
                conn.commit()

        if not dry_run:
            conn.commit()
        mode = "DRY-RUN" if dry_run else "APPLIED"
        print(f"[{mode}] scanned={scanned} updated={updated} sources={sorted(sources)}")
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill parsed_jobs seniority flags from role_title (handles sr./jr. punctuation)."
    )
    parser.add_argument(
        "--sources",
        default="builtin,workable,hiringcafe",
        help="Comma-separated raw_us_jobs sources to backfill",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    parser.add_argument("--batch-size", type=int, default=500, help="Commit every N updates")
    args = parser.parse_args()

    sources = {item.strip() for item in args.sources.split(",") if item.strip()}
    if not sources:
        raise RuntimeError("No sources provided")
    run(sources=sources, dry_run=bool(args.dry_run), batch_size=max(args.batch_size, 1))


if __name__ == "__main__":
    main()
