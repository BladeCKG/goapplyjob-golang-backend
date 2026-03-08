import argparse
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


def run(db_path: Path, dry_run: bool, limit_hosts: int | None) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT id, name, home_page_url
        FROM parsed_companies
        WHERE home_page_url IS NOT NULL AND trim(home_page_url) != ''
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

    merged_hosts = 0
    merged_companies = 0
    reassigned_jobs = 0

    for host in hosts:
        companies = sorted(by_host[host], key=lambda row: int(row["id"]))
        canonical = companies[0]
        duplicates = companies[1:]
        if not duplicates:
            continue
        merged_hosts += 1
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
        f"[{mode}] merged_hosts={merged_hosts} merged_companies={merged_companies} "
        f"reassigned_jobs={reassigned_jobs}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="One-time dedupe: merge parsed_companies by homepage host")
    parser.add_argument("--db", default="goapplyjob.db")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit-hosts", type=int, default=None)
    args = parser.parse_args()
    run(Path(args.db), args.dry_run, args.limit_hosts)


if __name__ == "__main__":
    main()
