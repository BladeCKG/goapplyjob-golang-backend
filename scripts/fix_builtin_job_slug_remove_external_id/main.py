import argparse
import re
import sqlite3
from pathlib import Path


def strip_builtin_external_id_suffix(slug: str | None) -> str | None:
    if not isinstance(slug, str):
        return None
    value = slug.strip().lower()
    if not value:
        return None
    cleaned = re.sub(r"-\d+$", "", value)
    cleaned = re.sub(r"-{2,}", "-", cleaned).strip("-")
    return cleaned or None


def run(db_path: Path, dry_run: bool, limit: int | None) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT p.id, p.slug
        FROM parsed_jobs p
        JOIN raw_us_jobs r ON r.id = p.raw_us_job_id
        WHERE r.source = 'builtin' AND p.slug IS NOT NULL AND trim(p.slug) != ''
        ORDER BY p.id ASC
        """
    ).fetchall()
    if limit and limit > 0:
        rows = rows[:limit]

    scanned = updated = unchanged = skipped_empty_after_clean = 0
    for row in rows:
        scanned += 1
        old_slug = row["slug"]
        new_slug = strip_builtin_external_id_suffix(old_slug)
        if not new_slug:
            skipped_empty_after_clean += 1
            continue
        if new_slug == old_slug:
            unchanged += 1
            continue
        cur.execute("UPDATE parsed_jobs SET slug = ? WHERE id = ?", (new_slug, row["id"]))
        updated += 1

    if dry_run:
        conn.rollback()
        mode = "DRY-RUN"
    else:
        conn.commit()
        mode = "APPLIED"
    conn.close()
    print(
        f"[{mode}] scanned={scanned} updated={updated} unchanged={unchanged} "
        f"skipped_empty_after_clean={skipped_empty_after_clean}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="One-time fix: remove trailing external-id suffix from BuiltIn parsed job slugs")
    parser.add_argument("--db", default="goapplyjob.db")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    run(Path(args.db), args.dry_run, args.limit)


if __name__ == "__main__":
    main()
