#!/usr/bin/env node
// Enrich the customer CSV with official websites from Wikidata.
//
// Strategy per row:
//   1. SPARQL match on Italian VAT (P3608).
//   2. Fallback: exact label match (Italian), taking P856 website.
// Output: clienti_con_sito.csv with added "Sito Web" and "Fonte" columns.
//
// Usage: node wikidata_lookup.js
// No external dependencies (Node 18+ for global fetch).

const fs = require("node:fs");
const path = require("node:path");

const INPUT = path.join(__dirname, "clienti_puliti (1).csv");
const OUTPUT = path.join(__dirname, "clienti_con_sito.csv");
const SPARQL_URL = "https://query.wikidata.org/sparql";
const USER_AGENT =
  "script-delco-enrich/1.0 " +
  "(https://github.com/malek-alhu/script-delco; contact via GitHub) " +
  "node-fetch";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function parseCsv(text) {
  const rows = [];
  let field = "";
  let row = [];
  let inQuotes = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQuotes) {
      if (c === '"' && text[i + 1] === '"') { field += '"'; i++; }
      else if (c === '"') { inQuotes = false; }
      else { field += c; }
    } else {
      if (c === '"') inQuotes = true;
      else if (c === ",") { row.push(field); field = ""; }
      else if (c === "\n") { row.push(field); rows.push(row); row = []; field = ""; }
      else if (c === "\r") { /* ignore */ }
      else field += c;
    }
  }
  if (field.length || row.length) { row.push(field); rows.push(row); }
  return rows.filter((r) => r.length && r.some((v) => v !== ""));
}

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

function clean(s) {
  return (s || "").replace(/\s+/g, " ").trim();
}

async function sparql(query, retries = 3) {
  const url = `${SPARQL_URL}?query=${encodeURIComponent(query)}&format=json`;
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const res = await fetch(url, {
        headers: {
          "User-Agent": USER_AGENT,
          Accept: "application/sparql-results+json",
        },
      });
      if (res.status === 429 || res.status === 503) {
        await sleep(1000 * 2 ** attempt);
        continue;
      }
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json();
    } catch (err) {
      if (attempt === retries - 1) throw err;
      await sleep(1000 * 2 ** attempt);
    }
  }
  return { results: { bindings: [] } };
}

async function lookupByVat(vat) {
  if (!vat) return null;
  const q = `
    SELECT ?item ?website WHERE {
      ?item wdt:P3608 "${vat}".
      OPTIONAL { ?item wdt:P856 ?website. }
    } LIMIT 1
  `;
  const data = await sparql(q);
  const b = data?.results?.bindings?.[0];
  return b?.website?.value || null;
}

async function lookupByName(name) {
  if (!name) return null;
  const safe = name.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
  const q = `
    SELECT ?item ?website WHERE {
      ?item rdfs:label "${safe}"@it.
      ?item wdt:P856 ?website.
    } LIMIT 1
  `;
  const data = await sparql(q);
  const b = data?.results?.bindings?.[0];
  return b?.website?.value || null;
}

async function main() {
  const raw = fs.readFileSync(INPUT, "utf8");
  const rows = parseCsv(raw);
  if (!rows.length) { console.error("Empty CSV"); process.exit(1); }

  const header = rows[0].map(clean);
  const idxVat = header.indexOf("Partita iva");
  const idxName = header.indexOf("Ragione Sociale");
  if (idxVat < 0 || idxName < 0) {
    console.error(`Header missing required columns. Got: ${JSON.stringify(header)}`);
    process.exit(1);
  }

  const outHeader = [...header, "Sito Web", "Fonte"];
  const out = fs.createWriteStream(OUTPUT, { encoding: "utf8" });
  out.write(outHeader.map(csvEscape).join(",") + "\n");

  const body = rows.slice(1);
  const total = body.length;
  console.error(`Processing ${total} rows...`);

  for (let i = 0; i < total; i++) {
    const cells = body[i].map(clean);
    const vat = cells[idxVat] || "";
    const name = cells[idxName] || "";
    let site = null;
    let source = "";

    try {
      site = await lookupByVat(vat);
      if (site) source = "wikidata:P3608";
      else {
        await sleep(200);
        site = await lookupByName(name);
        if (site) source = "wikidata:label";
      }
    } catch (err) {
      console.error(`[${i + 1}/${total}] ERROR ${name}: ${err.message}`);
    }

    const outRow = [...cells, site || "", source];
    while (outRow.length < outHeader.length) outRow.push("");
    out.write(outRow.map(csvEscape).join(",") + "\n");

    if ((i + 1) % 20 === 0 || site) {
      console.error(`[${i + 1}/${total}] ${JSON.stringify(name)} -> ${site || "(none)"}`);
    }
    await sleep(200);
  }

  out.end();
  await new Promise((r) => out.on("finish", r));
  console.error(`Done. Wrote ${OUTPUT}`);
}

main().catch((e) => { console.error(e); process.exit(1); });
