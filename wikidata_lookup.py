"""Enrich the customer CSV with official websites from Wikidata.

Lookup strategy per row:
1. Match by Italian VAT number (P3608).
2. Fallback: match by company name label (best-effort).
Found website is taken from P856.
"""
import csv
import sys
import time
from pathlib import Path

import requests

INPUT = Path(__file__).parent / "clienti_puliti (1).csv"
OUTPUT = Path(__file__).parent / "clienti_con_sito.csv"
SPARQL_URL = "https://query.wikidata.org/sparql"
HEADERS = {
    "User-Agent": (
        "script-delco-enrich/1.0 "
        "(https://github.com/malek-alhu/script-delco; contact via GitHub) "
        "python-requests"
    ),
    "Accept": "application/sparql-results+json",
}


def sparql(query: str, retries: int = 3) -> dict:
    for attempt in range(retries):
        try:
            r = requests.get(
                SPARQL_URL,
                params={"query": query, "format": "json"},
                headers=HEADERS,
                timeout=30,
            )
            if r.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException:
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)
    return {"results": {"bindings": []}}


def lookup_by_vat(vat: str) -> str | None:
    if not vat:
        return None
    q = f'''
    SELECT ?item ?website WHERE {{
      ?item wdt:P3608 "{vat}".
      OPTIONAL {{ ?item wdt:P856 ?website. }}
    }} LIMIT 1
    '''
    data = sparql(q)
    for b in data.get("results", {}).get("bindings", []):
        if "website" in b:
            return b["website"]["value"]
    return None


def lookup_by_name(name: str) -> str | None:
    if not name:
        return None
    safe = name.replace('"', '\\"')
    q = f'''
    SELECT ?item ?website WHERE {{
      ?item rdfs:label "{safe}"@it.
      ?item wdt:P856 ?website.
    }} LIMIT 1
    '''
    data = sparql(q)
    for b in data.get("results", {}).get("bindings", []):
        if "website" in b:
            return b["website"]["value"]
    return None


def clean(s: str) -> str:
    return " ".join(s.split()).strip() if s else ""


def main() -> int:
    with INPUT.open(newline="", encoding="utf-8") as fin:
        reader = csv.DictReader(fin)
        raw_fields = reader.fieldnames or []
        fieldnames = [f.strip() for f in raw_fields] + ["Sito Web", "Fonte"]
        rows_in = list(reader)

    total = len(rows_in)
    print(f"Processing {total} rows...", file=sys.stderr)

    with OUTPUT.open("w", newline="", encoding="utf-8") as fout:
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()

        for i, raw in enumerate(rows_in, 1):
            row = {k.strip(): clean(v) for k, v in raw.items()}
            vat = row.get("Partita iva", "")
            name = row.get("Ragione Sociale", "")
            site = None
            source = ""

            site = lookup_by_vat(vat)
            if site:
                source = "wikidata:P3608"
            else:
                time.sleep(0.2)
                site = lookup_by_name(name)
                if site:
                    source = "wikidata:label"

            row["Sito Web"] = site or ""
            row["Fonte"] = source
            writer.writerow(row)
            fout.flush()

            if i % 20 == 0 or site:
                print(f"[{i}/{total}] {name!r} -> {site or '(none)'}", file=sys.stderr)
            time.sleep(0.2)

    print(f"Done. Wrote {OUTPUT}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
