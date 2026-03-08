import argparse
import json
import os
import re
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib import request


DEFAULT_EMAILS_JSON = Path("test-extract/dev-bulk-sender/emails.json")
DEFAULT_TEMPLATE_PATH = Path("internal/email/templates/marketing_email.html")
DEFAULT_SITE_URL = "https://www.goapplyjob.online"
DEFAULT_SITE_NAME = "GoApplyJob"
DEFAULT_JOB_COUNT = 5
DEFAULT_SUBJECT = "New US Remote Software Engineer Jobs - GoApplyJob"
EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


@dataclass(frozen=True)
class MarketingJob:
    title: str
    company: str
    location: str
    salary: str
    posted_label: str
    job_url: str


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="One-time marketing sender for software engineer jobs."
    )
    parser.add_argument("--emails-json", type=Path, default=DEFAULT_EMAILS_JSON)
    parser.add_argument("--template", type=Path, default=DEFAULT_TEMPLATE_PATH)
    parser.add_argument("--subject", type=str, default=DEFAULT_SUBJECT)
    parser.add_argument("--site-url", type=str, default=DEFAULT_SITE_URL)
    parser.add_argument("--site-name", type=str, default=DEFAULT_SITE_NAME)
    parser.add_argument("--jobs-count", type=int, default=DEFAULT_JOB_COUNT)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def _database_path_from_env() -> str:
    raw = os.getenv("DATABASE_URL", "file:page_extract.db?_foreign_keys=on").strip()
    if raw.startswith("file:"):
        tail = raw[5:]
        q_idx = tail.find("?")
        return tail[:q_idx] if q_idx >= 0 else tail
    return raw


def _load_recipient_emails(path: Path) -> list[str]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    emails_raw = payload.get("emails")
    if not isinstance(emails_raw, list):
        raise ValueError("emails json must contain an 'emails' array")
    out: list[str] = []
    seen: set[str] = set()
    for item in emails_raw:
        if not isinstance(item, str):
            continue
        email = item.strip().lower()
        if not email or email in seen or not EMAIL_PATTERN.match(email):
            continue
        seen.add(email)
        out.append(email)
    return out


def _parse_json_array(raw: str | None) -> list[str]:
    if not raw:
        return []
    try:
        values = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(values, list):
        return []
    out: list[str] = []
    for value in values:
        if isinstance(value, str) and value.strip():
            out.append(value.strip())
    return out


def _query_counts(conn: sqlite3.Connection) -> tuple[int, int]:
    now = datetime.now(timezone.utc)
    day_cutoff = (now - timedelta(days=1)).isoformat()
    week_cutoff = (now - timedelta(days=7)).isoformat()
    daily = conn.execute(
        """
        SELECT COUNT(id)
        FROM parsed_jobs
        WHERE date_deleted IS NULL
          AND lower(trim(coalesce(categorized_job_function, ''))) = 'software engineer'
          AND coalesce(updated_at, '') >= ?
        """,
        (day_cutoff,),
    ).fetchone()[0]
    weekly = conn.execute(
        """
        SELECT COUNT(id)
        FROM parsed_jobs
        WHERE date_deleted IS NULL
          AND lower(trim(coalesce(categorized_job_function, ''))) = 'software engineer'
          AND coalesce(updated_at, '') >= ?
        """,
        (week_cutoff,),
    ).fetchone()[0]
    return int(daily or 0), int(weekly or 0)


def _query_jobs(conn: sqlite3.Connection, site_url: str, jobs_count: int) -> list[MarketingJob]:
    rows = conn.execute(
        """
        SELECT
          p.id,
          coalesce(p.role_title, 'Software Engineer'),
          coalesce(c.name, 'Unknown Company'),
          p.location_us_states,
          p.location_countries,
          p.salary_min_usd,
          p.salary_max_usd,
          p.salary_type,
          coalesce(p.created_at_source, p.updated_at)
        FROM parsed_jobs p
        LEFT JOIN parsed_companies c ON c.id = p.company_id
        WHERE p.date_deleted IS NULL
          AND lower(trim(coalesce(p.categorized_job_function, ''))) = 'software engineer'
        ORDER BY coalesce(p.created_at_source, p.updated_at) DESC, p.id DESC
        LIMIT ?
        """,
        (max(jobs_count, 1),),
    ).fetchall()
    out: list[MarketingJob] = []
    for row in rows:
        job_id = int(row[0])
        title = str(row[1])
        company = str(row[2])
        states = _parse_json_array(row[3])
        countries = _parse_json_array(row[4])
        salary_min = row[5]
        salary_max = row[6]
        salary_type = (row[7] or "").strip() if isinstance(row[7], str) else ""
        created = row[8] if isinstance(row[8], str) else ""
        location = "Remote"
        if states and countries:
            location = f"{', '.join(states)} ({', '.join(countries)})"
        elif states:
            location = ", ".join(states)
        elif countries:
            location = ", ".join(countries)
        salary = "Salary not listed"
        if salary_min is not None and salary_max is not None:
            suffix = f" / {salary_type}" if salary_type else ""
            salary = f"${float(salary_min):,.0f} - ${float(salary_max):,.0f}{suffix}"
        elif salary_min is not None:
            suffix = f" / {salary_type}" if salary_type else ""
            salary = f"${float(salary_min):,.0f}{suffix}"
        elif salary_max is not None:
            suffix = f" / {salary_type}" if salary_type else ""
            salary = f"${float(salary_max):,.0f}{suffix}"
        posted_label = "Recently updated"
        if created:
            posted_label = created[:10]
        job_url = f"{site_url.rstrip('/')}/jobs/{job_id}"
        out.append(
            MarketingJob(
                title=title,
                company=company,
                location=location,
                salary=salary,
                posted_label=posted_label,
                job_url=job_url,
            )
        )
    return out


def _build_jobs_block(jobs: list[MarketingJob]) -> str:
    blocks: list[str] = []
    for idx, job in enumerate(jobs, start=1):
        if idx > 1:
            blocks.append('<div style="height:10px;"></div>')
        blocks.append(
            (
                '<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
                'style="background:#111827;border:1px solid #1f2937;border-radius:12px;">'
                "<tr><td style=\"padding:14px 16px 5px;\">"
                f'<a href="{job.job_url}" style="font-size:16px;font-weight:700;line-height:1.4;color:#67e8f9;text-decoration:none;">{job.title}</a>'
                "</td></tr>"
                f'<tr><td style="padding:0 16px 14px;font-size:13px;line-height:1.6;color:#94a3b8;">{job.company} - {job.location} - {job.salary} - Posted {job.posted_label}</td></tr>'
                "</table>"
            )
        )
    return "\n".join(blocks)


def _build_html(
    template: str,
    recipient: str,
    site_url: str,
    jobs: list[MarketingJob],
    daily_new: int,
    weekly_new: int,
) -> str:
    first_name = recipient.split("@", 1)[0].replace(".", " ").replace("_", " ").strip().title() or "there"
    html = (
        template.replace("__FIRST_NAME__", first_name)
        .replace("__SITE_URL__", site_url.rstrip("/"))
        .replace("__SITE_LOGO_URL__", f"{site_url.rstrip('/')}/logo.png")
        .replace("__DAILY_NEW_SOFTWARE_ENGINEER_JOBS__", str(daily_new))
        .replace("__WEEKLY_NEW_SOFTWARE_ENGINEER_JOBS__", str(weekly_new))
        .replace("__BROWSE_JOBS_URL__", f"{site_url.rstrip('/')}/us-remote-jobs?job_title=Software+Engineer")
        .replace("__MANAGE_PREFERENCES_URL__", f"{site_url.rstrip('/')}/account")
        .replace("__UNSUBSCRIBE_URL__", f"{site_url.rstrip('/')}/account")
        .replace("__JOBS_BLOCK__", _build_jobs_block(jobs))
    )
    return html


def _build_text(site_url: str, jobs: list[MarketingJob]) -> str:
    lines = ["Latest US Remote Software Engineer Jobs", "", "Top picks:"]
    for idx, job in enumerate(jobs, start=1):
        lines.append(
            f"{idx}. {job.title} | {job.company} | {job.location} | {job.salary} | Posted {job.posted_label} | {job.job_url}"
        )
    lines.extend(["", f"Browse all: {site_url.rstrip('/')}/us-remote-jobs?job_title=Software+Engineer"])
    return "\n".join(lines)


def _send_via_brevo(to_email: str, subject: str, html_content: str, text_content: str) -> None:
    api_key = os.getenv("BREVO_API_KEY", "").strip()
    from_email = os.getenv("BREVO_FROM_EMAIL", "").strip()
    from_name = os.getenv("BREVO_FROM_NAME", "GoApplyJob").strip() or "GoApplyJob"
    api_url = os.getenv("BREVO_API_URL", "https://api.brevo.com/v3/smtp/email").strip()
    if not api_key or not from_email:
        raise RuntimeError("BREVO_API_KEY and BREVO_FROM_EMAIL are required when not using --dry-run")
    payload = json.dumps(
        {
            "sender": {"name": from_name, "email": from_email},
            "to": [{"email": to_email}],
            "subject": subject,
            "htmlContent": html_content,
            "textContent": text_content,
        }
    ).encode("utf-8")
    req = request.Request(api_url, data=payload, method="POST")
    req.add_header("accept", "application/json")
    req.add_header("api-key", api_key)
    req.add_header("content-type", "application/json")
    with request.urlopen(req, timeout=20) as resp:
        if resp.status >= 400:
            raise RuntimeError(f"brevo send failed status={resp.status}")


def main() -> int:
    args = _parse_args()
    if not args.emails_json.exists():
        raise FileNotFoundError(f"emails json not found: {args.emails_json}")
    if not args.template.exists():
        raise FileNotFoundError(f"template not found: {args.template}")

    recipients = _load_recipient_emails(args.emails_json)
    if not recipients:
        print("No valid recipient emails found.")
        return 1

    db_path = _database_path_from_env()
    conn = sqlite3.connect(db_path)
    try:
        daily_new, weekly_new = _query_counts(conn)
        jobs = _query_jobs(conn, args.site_url, args.jobs_count)
    finally:
        conn.close()
    if not jobs:
        raise RuntimeError("No software engineer jobs found.")

    template = args.template.read_text(encoding="utf-8")
    sent = 0
    failed = 0
    for recipient in recipients:
        html = _build_html(template, recipient, args.site_url, jobs, daily_new, weekly_new)
        text = _build_text(args.site_url, jobs)
        if args.dry_run:
            sent += 1
            continue
        try:
            _send_via_brevo(recipient, args.subject, html, text)
            sent += 1
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"[FAILED] {recipient}: {exc}")
    print(f"Done. total={len(recipients)} sent={sent} failed={failed} dry_run={args.dry_run}")
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
