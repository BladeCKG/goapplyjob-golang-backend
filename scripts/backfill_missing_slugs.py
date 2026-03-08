import argparse
import re
import sqlite3
from pathlib import Path
from urllib.parse import urlparse


def slugify(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return normalized or None


def slug_from_url_path(value: str | None) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    parsed = urlparse(value.strip())
    parts = [part for part in parsed.path.split("/") if part]
    if not parts:
        return None
    last = re.sub(r"-\d+$", "", parts[-1])
    return slugify(last or parts[-1])


def slug_from_url_host(value: str | None) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    host = (urlparse(value.strip()).hostname or "").lower().strip(".")
    if host.startswith("www."):
        host = host[4:]
    if not host:
        return None
    return slugify(host.split(".")[0])


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
            or slug_from_url_host(row["home_page_url"])
            or slug_from_url_host(row["linkedin_url"])
            or slugify(row["external_company_id"])
            or f"company-{row['id']}"
        )
        cur.execute("UPDATE parsed_companies SET slug = ? WHERE id = ?", (slug, row["id"]))
        updated += 1
    return scanned, updated


def backfill_jobs(cur: sqlite3.Cursor, limit: int | None) -> tuple[int, int]:
    rows = cur.execute(
        "SELECT id, role_title, url, external_job_id, slug FROM parsed_jobs WHERE slug IS NULL OR trim(slug) = '' ORDER BY id ASC"
    ).fetchall()
    if limit and limit > 0:
        rows = rows[:limit]
    scanned = updated = 0
    for row in rows:
        scanned += 1
        slug = (
            slugify(row["role_title"])
            or slug_from_url_path(row["url"])
            or slugify(row["external_job_id"])
            or f"job-{row['id']}"
        )
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
