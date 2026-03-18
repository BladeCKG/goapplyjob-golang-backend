import argparse
import re
import sqlite3
from pathlib import Path
from urllib.parse import urlparse


def normalized_host(url: str | None) -> str | None:
    if not isinstance(url, str) or not url.strip():
        return None
    host = (urlparse(url.strip()).hostname or "").lower().strip(".")
    if host.startswith("www."):
        host = host[4:]
    return host or None


def normalized_linkedin_identity(url: str | None) -> str | None:
    if not isinstance(url, str) or not url.strip():
        return None
    parsed = urlparse(url.strip() if "://" in url else f"https://{url.strip()}")
    host = (parsed.hostname or "").lower().strip(".")
    if host.startswith("www."):
        host = host[4:]
    if "linkedin.com" not in host:
        return None
    path = re.sub(r"/{2,}", "/", parsed.path or "/").strip("/")
    if path:
        return f"{host}/{path.lower()}"
    return host


def clean_slug_numeric_suffix(slug: str | None) -> str | None:
    if not isinstance(slug, str):
        return None
    cleaned = re.sub(r"-\d+$", "", slug.strip().lower())
    cleaned = re.sub(r"-{2,}", "-", cleaned).strip("-")
    return cleaned or None


def normalized_name_key(name: str | None) -> str | None:
    if not isinstance(name, str) or not name.strip():
        return None
    normalized = name.strip().lower().replace("&", " and ")
    normalized = re.sub(r"[^a-z0-9]+", "-", normalized)
    normalized = re.sub(r"-{2,}", "-", normalized).strip("-")
    return normalized or None


def normalized_slug_key(slug: str | None) -> str | None:
    if not isinstance(slug, str) or not slug.strip():
        return None
    return slug.strip().lower()


def run(db_path: Path, dry_run: bool, limit_hosts: int | None) -> None:
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

    by_host: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        host = normalized_host(row["home_page_url"])
        if not host:
            continue
        by_host.setdefault(host, []).append(row)

    hosts = [host for host, items in by_host.items() if len(items) > 1]
    hosts.sort()
    if limit_hosts and limit_hosts > 0:
        hosts = hosts[:limit_hosts]

    merged_groups = 0
    merged_companies = 0
    reassigned_jobs = 0
    slug_cleaned = 0

    for host in hosts:
        group = by_host[host]
        by_linkedin: dict[str, list[sqlite3.Row]] = {}
        null_linkedin: list[sqlite3.Row] = []
        for row in group:
            linkedin_id = normalized_linkedin_identity(row["linkedin_url"])
            if linkedin_id:
                by_linkedin.setdefault(linkedin_id, []).append(row)
            else:
                null_linkedin.append(row)

        merge_buckets: list[list[sqlite3.Row]] = []
        if len(by_linkedin) > 1:
            merge_buckets.extend(by_linkedin.values())
            if null_linkedin:
                merge_buckets.append(null_linkedin)
        elif len(by_linkedin) == 1:
            only_bucket = list(next(iter(by_linkedin.values())))
            only_bucket.extend(null_linkedin)
            merge_buckets.append(only_bucket)
        elif null_linkedin:
            merge_buckets.append(null_linkedin)

        for bucket in merge_buckets:
            companies = sorted(bucket, key=lambda row: int(row["id"]))
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
        description="One-time dedupe: merge parsed_companies by homepage host and isolate conflicting linkedin identities"
    )
    parser.add_argument("--db", default="goapplyjob.db")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit-hosts", type=int, default=None)
    args = parser.parse_args()
    run(Path(args.db), args.dry_run, args.limit_hosts)


if __name__ == "__main__":
    main()
