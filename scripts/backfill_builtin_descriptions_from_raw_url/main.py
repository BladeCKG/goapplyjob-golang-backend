import argparse
import json
import sqlite3
from pathlib import Path
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup


DESCRIPTION_FIELDS = (
    "roleDescription",
    "roleRequirements",
    "jobDescriptionSummary",
    "twoLineJobDescriptionSummary",
)


def fetch_html(url: str) -> str | None:
    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=20) as resp:
            return resp.read().decode("utf-8", errors="ignore")
    except Exception:
        return None


def to_plain_text(html: str | None) -> str | None:
    if not html:
        return None
    text = BeautifulSoup(html, "html.parser").get_text("\n")
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return "\n".join(lines) if lines else None


def extract_payload_fields(html_text: str, fallback_url: str) -> dict[str, str | None]:
    try:
        start = html_text.index("application/ld+json")
    except ValueError:
        return {}
    chunk = html_text[start : start + 200000]
    script_open = chunk.find(">")
    script_close = chunk.find("</script>")
    if script_open < 0 or script_close < 0:
        return {}
    raw_json = chunk[script_open + 1 : script_close].strip()
    try:
        payload = json.loads(raw_json)
    except Exception:
        return {}
    if isinstance(payload, list):
        payload = payload[0] if payload and isinstance(payload[0], dict) else {}
    if not isinstance(payload, dict):
        return {}
    if payload.get("@type") != "JobPosting":
        graph = payload.get("@graph")
        if isinstance(graph, list):
            payload = next((item for item in graph if isinstance(item, dict) and item.get("@type") == "JobPosting"), {})
    if not isinstance(payload, dict):
        return {}
    description_html = payload.get("description")
    description_text = to_plain_text(description_html if isinstance(description_html, str) else None)
    summary = (description_text[:280] + "...") if description_text and len(description_text) > 280 else description_text
    two_line = None
    if description_text:
        lines = [line.strip() for line in description_text.splitlines() if line.strip()]
        two_line = " ".join(lines[:2]) if lines else description_text
    return {
        "roleDescription": description_html if isinstance(description_html, str) and description_html.strip() else None,
        "roleRequirements": None,
        "jobDescriptionSummary": summary,
        "twoLineJobDescriptionSummary": two_line,
        "url": payload.get("url") if isinstance(payload.get("url"), str) else fallback_url,
    }


def run(db_path: Path, dry_run: bool, batch_size: int, limit: int | None, start_id: int | None) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    query = "SELECT id, url, raw_json FROM raw_us_jobs WHERE source = 'builtin'"
    args: list[object] = []
    if start_id is not None:
        query += " AND id >= ?"
        args.append(start_id)
    query += " ORDER BY id ASC"
    rows = cur.execute(query, args).fetchall()
    if limit and limit > 0:
        rows = rows[:limit]

    scanned = updated_raw = updated_parsed = skipped_fetch = skipped_parse = 0
    pending = 0
    for row in rows:
        scanned += 1
        url = row["url"]
        html = fetch_html(url)
        if not html:
            skipped_fetch += 1
            continue
        next_fields = extract_payload_fields(html, url)
        if not next_fields:
            skipped_parse += 1
            continue

        try:
            payload = json.loads(row["raw_json"] or "{}")
            if not isinstance(payload, dict):
                payload = {}
        except Exception:
            payload = {}
        changed = False
        for key in DESCRIPTION_FIELDS:
            if payload.get(key) != next_fields.get(key):
                payload[key] = next_fields.get(key)
                changed = True
        if changed:
            cur.execute("UPDATE raw_us_jobs SET raw_json = ? WHERE id = ?", (json.dumps(payload, ensure_ascii=False), row["id"]))
            updated_raw += 1

        parsed = cur.execute("SELECT id, role_description, role_requirements, job_description_summary, two_line_job_description_summary FROM parsed_jobs WHERE raw_us_job_id = ? LIMIT 1", (row["id"],)).fetchone()
        if parsed is not None:
            if (
                parsed["role_description"] != next_fields["roleDescription"]
                or parsed["role_requirements"] != next_fields["roleRequirements"]
                or parsed["job_description_summary"] != next_fields["jobDescriptionSummary"]
                or parsed["two_line_job_description_summary"] != next_fields["twoLineJobDescriptionSummary"]
            ):
                cur.execute(
                    """
                    UPDATE parsed_jobs
                    SET role_description = ?, role_requirements = ?, job_description_summary = ?, two_line_job_description_summary = ?
                    WHERE id = ?
                    """,
                    (
                        next_fields["roleDescription"],
                        next_fields["roleRequirements"],
                        next_fields["jobDescriptionSummary"],
                        next_fields["twoLineJobDescriptionSummary"],
                        parsed["id"],
                    ),
                )
                updated_parsed += 1
        pending += 1
        if not dry_run and pending % max(batch_size, 1) == 0:
            conn.commit()

    if dry_run:
        conn.rollback()
    else:
        conn.commit()
    conn.close()
    mode = "DRY-RUN" if dry_run else "APPLIED"
    print(
        f"[{mode}] scanned={scanned} updated_raw={updated_raw} updated_parsed={updated_parsed} "
        f"skipped_fetch={skipped_fetch} skipped_parse={skipped_parse}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill BuiltIn descriptions from raw URL with HTML-preserving role_description")
    parser.add_argument("--db", default="goapplyjob.db")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--start-id", type=int, default=None)
    args = parser.parse_args()
    run(Path(args.db), args.dry_run, args.batch_size, args.limit, args.start_id)


if __name__ == "__main__":
    main()
