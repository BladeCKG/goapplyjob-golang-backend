import argparse
import json
import sqlite3
from pathlib import Path
from urllib.request import Request, urlopen


def fetch_html(url: str) -> str:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=20) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def extract_profile_url(raw_json: str | None) -> str | None:
    if not raw_json:
        return None
    try:
        payload = json.loads(raw_json)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    company = payload.get("company")
    if not isinstance(company, dict):
        return None
    url = company.get("sourceCompanyProfileURL")
    if isinstance(url, str) and url.strip():
        return url.strip()
    return None


def extract_tagline_from_html(html: str) -> str | None:
    marker = 'x-ref="companyMission"'
    idx = html.find(marker)
    if idx < 0:
        return None
    chunk = html[idx : idx + 4000]
    cleaned = (
        chunk.replace("<br>", "\n")
        .replace("<br/>", "\n")
        .replace("<br />", "\n")
        .replace("</p>", "\n")
        .replace("</div>", "\n")
    )
    import re

    cleaned = re.sub(r"(?is)<[^>]+>", "", cleaned)
    lines = []
    for line in cleaned.splitlines():
        line = " ".join(line.split()).strip()
        if line:
            lines.append(line)
    if not lines:
        return None
    return "\n".join(lines[:6])


def run(db_path: Path, dry_run: bool, limit: int | None) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT c.id AS company_id, c.tagline, r.url AS job_url, r.raw_json
        FROM parsed_companies c
        JOIN parsed_jobs p ON p.company_id = c.id
        JOIN raw_us_jobs r ON r.id = p.raw_us_job_id
        WHERE r.source = 'builtin' AND (c.tagline IS NULL OR trim(c.tagline) = '')
        ORDER BY c.id ASC, r.id DESC
        """
    ).fetchall()
    by_company: dict[int, dict[str, list[str] | str | None]] = {}
    for row in rows:
        company_id = int(row["company_id"])
        item = by_company.setdefault(company_id, {"profile_url": None, "job_urls": []})
        profile_url = extract_profile_url(row["raw_json"])
        if profile_url and not item["profile_url"]:
            item["profile_url"] = profile_url
        job_url = row["job_url"]
        if isinstance(job_url, str) and job_url.strip():
            urls = item["job_urls"]
            if job_url not in urls:
                urls.append(job_url)
    company_ids = sorted(by_company.keys())
    if limit and limit > 0:
        company_ids = company_ids[:limit]

    updated = 0
    for company_id in company_ids:
        info = by_company[company_id]
        tagline = None
        profile_url = info["profile_url"]
        if isinstance(profile_url, str) and profile_url:
            try:
                tagline = extract_tagline_from_html(fetch_html(profile_url))
            except Exception:
                tagline = None
        if not tagline:
            for job_url in info["job_urls"]:
                try:
                    tagline = extract_tagline_from_html(fetch_html(job_url))
                except Exception:
                    tagline = None
                if tagline:
                    break
        if not tagline:
            continue
        updated += 1
        cur.execute("UPDATE parsed_companies SET tagline = ? WHERE id = ?", (tagline, company_id))
    if dry_run:
        conn.rollback()
    else:
        conn.commit()
    conn.close()
    mode = "DRY-RUN" if dry_run else "APPLIED"
    print(f"[{mode}] companies_scanned={len(company_ids)} updated={updated}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill BuiltIn parsed_companies.tagline from mission block HTML")
    parser.add_argument("--db", default="goapplyjob.db", help="Path to sqlite database")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    run(Path(args.db), args.dry_run, args.limit)


if __name__ == "__main__":
    main()
