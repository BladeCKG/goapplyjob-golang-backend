import csv
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
EXTRA = ROOT / 'extra'
INTERNAL = ROOT / 'internal' / 'locationnorm'
COUNTRIES_CSV = EXTRA / 'wikipedia-iso-country-codes.csv'
STATES_CSV = EXTRA / 'states.csv'
OUTPUT_JSON = INTERNAL / 'location-normalization.json'


def _key(value: str) -> str:
    return re.sub(r'[^A-Z0-9]+', ' ', value.upper()).strip()


def _title(value: str) -> str:
    return ' '.join(part.capitalize() for part in value.strip().lower().split())


def _clean_text(value: str) -> str:
    raw = value.strip()
    if not raw:
        return raw
    try:
        repaired = raw.encode('latin1').decode('utf-8')
        if repaired:
            return repaired
    except Exception:
        pass
    return raw


def build() -> dict:
    countries_by_alpha2: dict[str, str] = {}
    countries_by_alpha3: dict[str, str] = {}
    countries_by_name: dict[str, str] = {}

    with COUNTRIES_CSV.open('r', encoding='utf-8', errors='ignore', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_name = _clean_text(row.get('English short name lower case') or '')
            a2 = (row.get('Alpha-2 code') or '').strip().upper()
            a3 = (row.get('Alpha-3 code') or '').strip().upper()
            if not raw_name:
                continue
            display_name = _title(raw_name)
            countries_by_name[_key(raw_name)] = display_name
            if a2:
                countries_by_alpha2[a2] = display_name
            if a3:
                countries_by_alpha3[a3] = display_name

    country_aliases = {
        'USA': 'United States',
        'U S': 'United States',
        'U S A': 'United States',
        'UNITED STATES OF AMERICA': 'United States',
        'UK': 'United Kingdom',
        'GREAT BRITAIN': 'United Kingdom',
    }

    states_by_abbreviation: dict[str, str] = {}
    states_by_name: dict[str, str] = {}
    with STATES_CSV.open('r', encoding='utf-8', errors='ignore', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = _clean_text(row.get('State') or '')
            abbr = (row.get('Abbreviation') or '').strip().upper()
            if not name or not abbr:
                continue
            display_name = _title(name)
            states_by_abbreviation[abbr] = display_name
            states_by_name[_key(name)] = display_name

    return {
        'countries': {
            'by_alpha2': countries_by_alpha2,
            'by_alpha3': countries_by_alpha3,
            'by_name': countries_by_name,
            'aliases': country_aliases,
        },
        'us_states': {
            'by_abbreviation': states_by_abbreviation,
            'by_name': states_by_name,
        },
    }


def main() -> None:
    payload = build()
    OUTPUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(f'wrote: {OUTPUT_JSON}')


if __name__ == '__main__':
    main()
