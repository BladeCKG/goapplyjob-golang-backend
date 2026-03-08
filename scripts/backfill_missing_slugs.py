import argparse
import re
import sqlite3
from pathlib import Path
from urllib.parse import urlparse


def slugify(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    if not normalized or not re.search(r"[a-z]", normalized):
        return None
    return normalized


def slug_from_url_path(value: str | None) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    parsed = urlparse(value.strip())
    parts = [part for part in parsed.path.split("/") if part]
    if not parts:
        return None
    last = parts[-1]
    if last.isdigit() and len(parts) >= 2:
        last = parts[-2]
    last = re.sub(r"-\d+$", "", last)
    return slugify(last or parts[-1])


def backfill_companies(cur: sqlite3.Cursor, limit: int | None) -> tuple[int, int]:
    rows = cur.execute(
        "SELECT id, name, home_page_url, linkedin_url, external_company_id, slug FROM parsed_companies WHERE slug IS NULL OR trim(slug) = '' ORDER BY id ASC"
    ).fetchall()
    if limit and limit > 0:
        rows = rows[:limit]
    scanned = updated = 0
    for row in rows:
        scanned += 1
        slug = (
            slugify(row["name"])
            or f"company-{row['id']}"
        )
        cur.execute("UPDATE parsed_companies SET slug = ? WHERE id = ?", (slug, row["id"]))
        updated += 1
    return scanned, updated


def build_job_slug(role_title: str | None, source: str | None, raw_url: str | None, job_id: int) -> str:
    normalized_source = (source or "").strip().lower()
    if normalized_source in {"builtin", "remotive"}:
        return slug_from_url_path(raw_url) or slugify(role_title) or f"job-{job_id}"
    return slugify(role_title) or slug_from_url_path(raw_url) or f"job-{job_id}"


def backfill_jobs(cur: sqlite3.Cursor, limit: int | None) -> tuple[int, int]:
    rows = cur.execute(
        """
        SELECT p.id, p.role_title, p.slug, r.source, r.url
        FROM parsed_jobs p
        JOIN raw_us_jobs r ON r.id = p.raw_us_job_id
        WHERE p.slug IS NULL OR trim(p.slug) = ''
        ORDER BY p.id ASC
        """
    ).fetchall()
    if limit and limit > 0:
        rows = rows[:limit]
    scanned = updated = 0
    for row in rows:
        scanned += 1
        slug = build_job_slug(row["role_title"], row["source"], row["url"], row["id"])
        cur.execute("UPDATE parsed_jobs SET slug = ? WHERE id = ?", (slug, row["id"]))
        updated += 1
    return scanned, updated


def run(db_path: Path, dry_run: bool, limit: int | None) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    company_scanned, company_updated = backfill_companies(cur, limit)
    job_scanned, job_updated = backfill_jobs(cur, limit)
    if dry_run:
        conn.rollback()
        mode = "DRY-RUN"
    else:
        conn.commit()
        mode = "APPLIED"
    conn.close()
    print(
        f"[{mode}] companies_scanned={company_scanned} companies_updated={company_updated} "
        f"jobs_scanned={job_scanned} jobs_updated={job_updated}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill missing parsed_companies.slug and parsed_jobs.slug")
    parser.add_argument("--db", default="goapplyjob.db")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    run(Path(args.db), args.dry_run, args.limit)


if __name__ == "__main__":
    main()
