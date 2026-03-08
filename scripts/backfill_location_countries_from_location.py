import argparse
import asyncio
import json
from pathlib import Path
import sys

from dotenv import load_dotenv
from sqlalchemy import bindparam, select, update

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.db.models import ParsedJob, RawUsJob
from app.db.session import SessionLocal
from app.workers.location_normalization import normalize_country_name


def _normalize_location_countries_values(location_countries: object) -> list[str]:
    raw_values: list[str] = []
    if isinstance(location_countries, list):
        raw_values = [item for item in location_countries if isinstance(item, str)]
    elif isinstance(location_countries, str):
        text = location_countries.strip()
        if not text:
            return []
        # Support accidental JSON-stringified arrays.
        try:
            parsed = json.loads(text)
        except Exception:
            parsed = None
        if isinstance(parsed, list):
            raw_values = [item for item in parsed if isinstance(item, str)]
        else:
            raw_values = [text]

    if not raw_values:
        return []

    values: list[str] = []
    seen: set[str] = set()
    segments: list[str] = []
    for raw in raw_values:
        parts = [segment.strip() for segment in raw.split("|") if segment.strip()]
        if parts:
            segments.extend(parts)

    for segment in segments:
        candidate = segment.strip()
        if not candidate:
            continue
        normalized = normalize_country_name(candidate, allow_fallback_title_case=True)
        if not normalized:
            continue
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        values.append(normalized)
    return values


async def run(
    *,
    sources: set[str] | None,
    overwrite: bool,
    dry_run: bool,
    batch_size: int,
) -> None:
    scanned = 0
    updated = 0
    pending_rows: list[dict[str, object]] = []

    async with SessionLocal() as session:
        query = select(ParsedJob.id, ParsedJob.location_countries)
        if sources:
            query = (
                select(ParsedJob.id, ParsedJob.location_countries)
                .join(RawUsJob, RawUsJob.id == ParsedJob.raw_us_job_id)
                .where(RawUsJob.source.in_(tuple(sources)))
            )
        query = query.order_by(ParsedJob.id.asc()).execution_options(yield_per=max(batch_size, 100))
        stream = await session.stream(query)

        update_stmt = (
            update(ParsedJob.__table__)
            .where(ParsedJob.__table__.c.id == bindparam("job_id"))
            .values(location_countries=bindparam("location_countries"))
        )

        async for row in stream:
            scanned += 1
            parsed_id = row.id
            existing = row.location_countries

            countries = _normalize_location_countries_values(existing)
            if not countries:
                continue

            if isinstance(existing, list) and existing == countries:
                continue

            updated += 1
            if not dry_run:
                pending_rows.append({"job_id": parsed_id, "location_countries": countries})
            if not dry_run and len(pending_rows) >= batch_size:
                await session.execute(update_stmt, pending_rows)
                await session.commit()
                pending_rows.clear()

        if not dry_run and pending_rows:
            await session.execute(update_stmt, pending_rows)
            await session.commit()

    mode = "DRY-RUN" if dry_run else "APPLIED"
    source_info = sorted(sources) if sources else ["all"]
    print(f"[{mode}] scanned={scanned} updated={updated} sources={source_info} overwrite={overwrite}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="One-time backfill: normalize parsed_jobs.location_countries in-place"
    )
    parser.add_argument(
        "--sources",
        default="",
        help="Optional comma-separated sources to scope rows (example: builtin,workable). Default: all.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Deprecated no-op (kept for CLI compatibility). Existing location_countries values are normalized by default.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview only; do not commit changes")
    parser.add_argument("--batch-size", type=int, default=500, help="Commit every N updates (default: 500)")
    args = parser.parse_args()

    load_dotenv(PROJECT_ROOT / ".env")

    sources = {item.strip() for item in args.sources.split(",") if item.strip()} or None
    asyncio.run(
        run(
            sources=sources,
            overwrite=bool(args.overwrite),
            dry_run=bool(args.dry_run),
            batch_size=max(args.batch_size, 1),
        )
    )


if __name__ == "__main__":
    main()
