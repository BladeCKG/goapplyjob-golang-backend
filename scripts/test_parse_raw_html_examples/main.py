import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.workers.plugins import get_source_plugin
import app.workers.sources.builtin as builtin_source
from app.workers.sources.workable import parse_import_rows_from_workable_payload_text


FIXTURES = ROOT / "test-extract"
OUT_DIR = FIXTURES / "generated-raw-json"


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _run_builtin() -> dict[str, Any]:
    plugin = get_source_plugin("builtin")
    if plugin is None:
        return {"error": "missing plugin: builtin"}

    job_html = _read_text(FIXTURES / "builtin" / "raw-job-1.html")
    company_html = _read_text(FIXTURES / "builtin" / "raw-job-1-company.html")

    original_fetch_html = builtin_source._fetch_html
    try:
        builtin_source._fetch_html = lambda _url: company_html  # type: ignore[assignment]
        parsed = plugin.parse_raw_html(job_html, "https://builtin.com/job/u-s-advanced-analytics-manager/4523780")
        return parsed if isinstance(parsed, dict) else {}
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}
    finally:
        builtin_source._fetch_html = original_fetch_html  # type: ignore[assignment]


def _run_remoterocketship() -> dict[str, Any]:
    plugin = get_source_plugin("remoterocketship")
    if plugin is None:
        return {"error": "missing plugin: remoterocketship"}
    html = _read_text(FIXTURES / "remoterocketship" / "page.html")
    try:
        parsed = plugin.parse_raw_html(
            html,
            "https://www.remoterocketship.com/us/company/cvs-health/jobs/staff-software-development-engineer-united-states-remote/",
        )
        return parsed if isinstance(parsed, dict) else {}
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}


def _run_workable() -> dict[str, Any]:
    payload_text = _read_text(FIXTURES / "workable" / "job-listings.json")
    try:
        rows, _skipped = parse_import_rows_from_workable_payload_text(payload_text)
        if not rows:
            return {}
        _url, _post_date, raw_payload = rows[0]
        return raw_payload if isinstance(raw_payload, dict) else {}
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}


def _run_hiringcafe() -> dict[str, Any]:
    plugin = get_source_plugin("hiringcafe")
    if plugin is None:
        return {"error": "missing plugin: hiringcafe"}
    html = _read_text(FIXTURES / "hiring-cafe" / "viewjob_cloudscraper.html")
    try:
        parsed = plugin.parse_raw_html(html, "https://hiring.cafe/viewjob/ipra1n7xb7s4l54h")
        if isinstance(parsed, dict):
            return parsed
        return {}
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    outputs = {
        "builtin.parse_raw_html.json": _run_builtin(),
        "remoterocketship.parse_raw_html.json": _run_remoterocketship(),
        "workable.parse_raw_html.json": _run_workable(),
        "hiringcafe.parse_raw_html.json": _run_hiringcafe(),
    }

    for name, payload in outputs.items():
        _write_json(OUT_DIR / name, payload)

    summary = {
        "files": sorted(outputs.keys()),
        "notes": {
            "workable": "Uses workable job-listings fixture via import/watcher parser to produce raw payload.",
            "hiringcafe": "parse_raw_html intentionally returns {} (watcher/import provides raw payload).",
        },
    }
    _write_json(OUT_DIR / "parse_raw_html.index.json", summary)
    print(f"Generated parse_raw_html outputs in: {OUT_DIR}")


if __name__ == "__main__":
    main()
