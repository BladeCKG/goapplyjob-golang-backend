import argparse
import sqlite3
from pathlib import Path

from merge_duplicate_companies_by_homepage_host import (
    clean_slug_numeric_suffix,
    normalized_host,
    normalized_linkedin_identity,
    normalized_name_key,
    normalized_slug_key,
)


def run(db_path: Path, dry_run: bool, limit_groups: int | None) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT id, name, slug, home_page_url, linkedin_url
        FROM parsed_companies
        ORDER BY id ASC
        """
    ).fetchall()

    groups: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        linkedin_key = normalized_linkedin_identity(row["linkedin_url"])
        if linkedin_key:
            groups.setdefault(f"li:{linkedin_key}", []).append(row)
        name_key = normalized_name_key(row["name"])
        slug_key = normalized_slug_key(row["slug"])
        host_key = normalized_host(row["home_page_url"])
        if name_key and slug_key and host_key:
            groups.setdefault(f"nsh:{name_key}|{slug_key}|{host_key}", []).append(row)

    candidate_keys = [key for key, items in groups.items() if len(items) > 1]
    candidate_keys.sort()
    if limit_groups and limit_groups > 0:
        candidate_keys = candidate_keys[:limit_groups]

    merged_groups = 0
    merged_companies = 0
    reassigned_jobs = 0
    slug_cleaned = 0

    for key in candidate_keys:
        companies = sorted(groups[key], key=lambda row: int(row["id"]))
        canonical = companies[0]
        cleaned_slug = clean_slug_numeric_suffix(canonical["slug"])
        if cleaned_slug and cleaned_slug != canonical["slug"]:
            cur.execute("UPDATE parsed_companies SET slug = ? WHERE id = ?", (cleaned_slug, int(canonical["id"])))
            slug_cleaned += cur.rowcount if cur.rowcount is not None else 0

        duplicates = companies[1:]
        if not duplicates:
            continue

        merged_groups += 1
        for dup in duplicates:
            dup_id = int(dup["id"])
            canonical_id = int(canonical["id"])
            cur.execute("UPDATE parsed_jobs SET company_id = ? WHERE company_id = ?", (canonical_id, dup_id))
            reassigned_jobs += cur.rowcount if cur.rowcount is not None else 0
            cur.execute("DELETE FROM parsed_companies WHERE id = ?", (dup_id,))
            merged_companies += cur.rowcount if cur.rowcount is not None else 0

    if dry_run:
        conn.rollback()
        mode = "DRY-RUN"
    else:
        conn.commit()
        mode = "APPLIED"
    conn.close()
    print(
        f"[{mode}] merged_groups={merged_groups} merged_companies={merged_companies} "
        f"reassigned_jobs={reassigned_jobs} slug_cleaned={slug_cleaned}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="One-time dedupe: merge parsed_companies by linkedin identity and name+slug+homepage host"
    )
    parser.add_argument("--db", default="goapplyjob.db")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit-groups", type=int, default=None)
    args = parser.parse_args()
    run(Path(args.db), args.dry_run, args.limit_groups)


if __name__ == "__main__":
    main()
