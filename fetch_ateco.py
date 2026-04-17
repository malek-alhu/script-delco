import csv
import json
import logging
import time
import os

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

INPUT_CSV = "clienti_puliti (1).csv"
OUTPUT_CSV = "clienti_con_ateco.csv"
CACHE_FILE = "ateco_cache.json"
LOG_FILE = "fetch_ateco.log"
BASE_URL = "https://registroaziende.it"
DELAY = 1.5  # seconds between requests

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s %(message)s",
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cache(cache):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def fetch(session, url, retries=1):
    for attempt in range(retries + 1):
        try:
            resp = session.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            return resp.text
        except Exception as exc:
            if attempt < retries:
                time.sleep(3)
            else:
                raise exc


def parse_search(html, piva):
    soup = BeautifulSoup(html, "html.parser")
    link = soup.select_one('a[href*="/azienda/"]')
    if not link:
        logging.warning("No result found for P.IVA %s", piva)
        return None
    return BASE_URL + link["href"]


def split_ateco(value):
    """Split '62.1: Attività di programmazione' → ('62.1', 'Attività di programmazione')"""
    if not value:
        return "", ""
    if ":" in value:
        code, _, desc = value.partition(":")
        return code.strip(), desc.strip()
    return value.strip(), ""


def parse_detail(html):
    soup = BeautifulSoup(html, "html.parser")
    result = {
        "codice_2025": "",
        "desc_2025": "",
        "codice_primario": "",
        "desc_primario": "",
    }

    # Data lives in <tr><th scope="row">Label</th><td>Value</td></tr>
    for tr in soup.find_all("tr"):
        th = tr.find("th")
        td = tr.find("td")
        if not th or not td:
            continue
        label = th.get_text(strip=True)
        value = td.get_text(strip=True)
        if "ATECO 2025" in label:
            result["codice_2025"], result["desc_2025"] = split_ateco(value)
        elif "ATECO Primario" in label:
            result["codice_primario"], result["desc_primario"] = split_ateco(value)

    return result


def scrape_piva(session, piva, cache):
    if piva in cache:
        return cache[piva]

    data = {"codice_2025": "", "desc_2025": "", "codice_primario": "", "desc_primario": ""}

    try:
        search_html = fetch(session, f"{BASE_URL}/ricerca?q={piva}")
        detail_url = parse_search(search_html, piva)
        if not detail_url:
            cache[piva] = data
            return data
        time.sleep(0.5)
        detail_html = fetch(session, detail_url)
        data = parse_detail(detail_html)
    except Exception as exc:
        logging.error("Error for P.IVA %s: %s", piva, exc)

    cache[piva] = data
    return data


def main():
    cache = load_cache()

    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames

    new_fields = [
        "Codice ATECO 2025",
        "Descrizione ATECO 2025",
        "Codice ATECO Primario",
        "Descrizione ATECO Primario",
    ]
    out_fields = list(fieldnames) + new_fields

    session = requests.Session()

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=out_fields)
        writer.writeheader()

        for row in tqdm(rows, desc="Fetching ATECO codes"):
            piva = row.get("Partita iva", "").strip()

            if piva:
                data = scrape_piva(session, piva, cache)
                save_cache(cache)
                time.sleep(DELAY)
            else:
                data = {"codice_2025": "", "desc_2025": "", "codice_primario": "", "desc_primario": ""}
                logging.warning("Empty P.IVA in row: %s", row)

            row["Codice ATECO 2025"] = data["codice_2025"]
            row["Descrizione ATECO 2025"] = data["desc_2025"]
            row["Codice ATECO Primario"] = data["codice_primario"]
            row["Descrizione ATECO Primario"] = data["desc_primario"]
            writer.writerow(row)

    print(f"\nDone. Output: {OUTPUT_CSV}")
    print(f"Cache: {CACHE_FILE} ({len(cache)} entries)")
    print(f"Errors logged to: {LOG_FILE}")


if __name__ == "__main__":
    main()
