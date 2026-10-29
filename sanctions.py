#!/usr/bin/env python3
"""Download and merge official sanctions lists for the US, Switzerland and Germany.

Only sanctioned individuals (natural persons) are kept. Companies, organisations,
vessels and aircraft are dropped from the merged output. Entries whose source gives
no type at all are kept and labelled "unspecified" rather than silently dropped.

Usage:
    python3 sanctions.py                 # download everything and merge
    python3 sanctions.py --offline       # re-merge the most recent downloads
    python3 sanctions.py --sources ofac_sdn,seco

Only the Python standard library is used (Python 3.9+).

Sources
-------
  ofac_sdn   US      OFAC Specially Designated Nationals (SDN) list
  ofac_cons  US      OFAC Consolidated (non-SDN) sanctions list
  seco       CH      Swiss SECO consolidated sanctions list
  eu         EU/DE   EU Consolidated Financial Sanctions list. Germany has no
                     standalone national list: it enforces EU and UN sanctions,
                     so this list is what applies in Germany.
  un         UN      UN Security Council consolidated list (binding on all
                     three jurisdictions; largely mirrored by EU and SECO)

Outputs (in --out, default ./output/YYYY-MM-DD_HH-MM-SS_UTC)
----------------------------------------------------------
  sanctions_merged.csv    one row per listed individual per source, common schema
  sanctions_merged.json   same data as JSON (list fields kept as arrays)
  crypto_addresses.csv    every digital currency address attributed to a listed individual
  logs.txt                everything the script logged during the run
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
import shutil
import sys
import threading
import time
import traceback
import unicodedata
import urllib.request
import xml.etree.ElementTree as ET
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

SOURCES = {
    "ofac_sdn": {
        "jurisdiction": "US",
        "name": "OFAC SDN List",
        "url": "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.XML",
    },
    "ofac_cons": {
        "jurisdiction": "US",
        "name": "OFAC Consolidated (non-SDN) List",
        "url": "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/CONSOLIDATED.XML",
    },
    "seco": {
        "jurisdiction": "CH",
        "name": "SECO Swiss Consolidated Sanctions List",
        "url": "https://www.sesam.search.admin.ch/sesam-search-web/pages/downloadXmlGesamtliste.xhtml"
        "?lang=en&action=downloadXmlGesamtlisteAction",
    },
    "eu": {
        "jurisdiction": "EU/DE",
        "name": "EU Consolidated Financial Sanctions List",
        # The token is the public one published on the EU FSF download page.
        "url": "https://webgate.ec.europa.eu/fsd/fsf/public/files/xmlFullSanctionsList_1_1/content"
        "?token=dG9rZW4tMjAxNw",
    },
    "un": {
        "jurisdiction": "UN",
        "name": "UN Security Council Consolidated List",
        "url": "https://scsanctions.un.org/resources/xml/en/consolidated.xml",
    },
}

FIELDS = [
    "source",
    "jurisdiction",
    "list_id",
    "entity_type",
    "name",
    "aliases",
    "programs",
    "dates_of_birth",
    "places_of_birth",
    "nationalities",
    "address_countries",
    "identifiers",
    "crypto_addresses",
    "listed_on",
    "remarks",
    "also_listed_in",
]
LIST_FIELDS = [
    "aliases",
    "programs",
    "dates_of_birth",
    "places_of_birth",
    "nationalities",
    "address_countries",
    "identifiers",
    "crypto_addresses",
    "also_listed_in",
]

USER_AGENT = "sanction-list-merger/1.0 (+https://github.com/)"
UNSPECIFIED = "unspecified"  # entity_type for entries the source does not classify
KEEP_TYPES = ("individual", UNSPECIFIED)
EVM_ADDRESS = re.compile(r"\b0x[0-9a-fA-F]{40}\b")


LOG_FILE = None  # output/logs.txt; opened in main() once the output directory is known
LOG_LOCK = threading.Lock()  # downloads log from worker threads


def log(msg: str) -> None:
    with LOG_LOCK:
        print(msg, file=sys.stderr, flush=True)
        if LOG_FILE is not None:
            LOG_FILE.write(msg + "\n")
            LOG_FILE.flush()


def tag(key: str) -> str:
    """Log prefix for a source, e.g. '[us]' or '[ch]'."""
    return f"[{SOURCES[key]['jurisdiction'].lower()}]"


TIME_FMT = "%Y-%m-%d %H:%M:%S UTC"  # in log lines
FOLDER_TIME_FMT = "%Y-%m-%d_%H-%M-%S_UTC"  # in the output folder name: no colons or spaces


def now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime(TIME_FMT)


# --------------------------------------------------------------------------
# Download
# --------------------------------------------------------------------------


def download(key: str, dest: Path, retries: int, timeout: int) -> Path:
    """Download one source to dest, retrying until the file parses as XML.

    The SECO server is slow and sometimes drops the connection mid-transfer
    without signalling an error, so every download is checked by parsing it.
    """
    url = SOURCES[key]["url"]
    tmp = dest.with_suffix(".part")
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=timeout) as resp, open(tmp, "wb") as fh:
                shutil.copyfileobj(resp, fh, length=1 << 20)
            ET.parse(tmp)  # raises if the transfer was truncated
            tmp.replace(dest)
            log(f"{tag(key)} sanctions list downloaded from {url} at {now()}")
            return dest
        except Exception as exc:  # network errors, truncated XML, HTTP errors
            log(f"{tag(key)} download attempt {attempt}/{retries} from {url} failed at {now()}: "
                f"{type(exc).__name__}: {exc}")
            if attempt < retries:
                time.sleep(min(5 * attempt, 30))
    tmp.unlink(missing_ok=True)
    raise RuntimeError(f"could not download {url} after {retries} attempts")


def download_all(keys: list[str], raw_dir: Path, retries: int, timeout: int) -> dict[str, Path]:
    raw_dir.mkdir(parents=True, exist_ok=True)
    log(f"Downloading {len(keys)} list(s) to {raw_dir} (the Swiss list can take a few minutes)...")
    paths, failed = {}, []
    with ThreadPoolExecutor(max_workers=len(keys)) as pool:
        futures = {
            pool.submit(download, k, raw_dir / f"{k}.xml", retries, timeout): k for k in keys
        }
        for fut in as_completed(futures):
            key = futures[fut]
            try:
                paths[key] = fut.result()
            except Exception as exc:
                log(f"{tag(key)} ERROR: {exc}")
                failed.append(key)
    if failed:
        log(f"WARNING: failed sources will be missing from the merge: {', '.join(failed)}")
    return paths


def latest_downloads(keys: list[str], data_dir: Path) -> dict[str, Path]:
    """For --offline: find the newest downloaded copy of each source."""
    paths = {}
    for key in keys:
        candidates = sorted(data_dir.glob(f"*/{key}.xml"))
        if candidates:
            paths[key] = candidates[-1]
            log(f"{tag(key)} using previously downloaded {candidates[-1]}")
        else:
            log(f"{tag(key)} no local copy of {SOURCES[key]['name']} found, skipping")
    return paths


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def load_xml(path: Path) -> ET.Element:
    """Parse XML and strip namespaces so tags can be matched by local name."""
    root = ET.parse(path).getroot()
    for el in root.iter():
        if isinstance(el.tag, str) and "}" in el.tag:
            el.tag = el.tag.split("}", 1)[1]
    return root


def text(el: ET.Element | None, path: str = "") -> str:
    if el is None:
        return ""
    found = el.find(path) if path else el
    return (found.text or "").strip() if found is not None else ""


def join(*parts: str) -> str:
    return " ".join(p.strip() for p in parts if p and p.strip())


def uniq(items) -> list[str]:
    seen, out = set(), []
    for item in items:
        item = (item or "").strip()
        if item and item.lower() not in seen:
            seen.add(item.lower())
            out.append(item)
    return out


def record(source: str, **fields) -> dict:
    rec = {f: [] if f in LIST_FIELDS else "" for f in FIELDS}
    rec["source"] = source
    rec["jurisdiction"] = SOURCES[source]["jurisdiction"]
    rec.update(fields)
    rec["entity_type"] = rec["entity_type"] or UNSPECIFIED
    for f in LIST_FIELDS:
        rec[f] = uniq(rec[f])
    rec["aliases"] = [a for a in rec["aliases"] if a.lower() != rec["name"].lower()]
    return rec


def evm_addresses(*texts: str) -> list[str]:
    """Find 0x addresses in free text. Labelled EVM since the chain is often unstated."""
    return [f"EVM: {m}" for t in texts for m in EVM_ADDRESS.findall(t or "")]


def ofac_date(value: str) -> str:
    """'10 Dec 1948' -> '1948-12-10'; partial or ranged dates are kept as-is."""
    try:
        return dt.datetime.strptime(value, "%d %b %Y").date().isoformat()
    except ValueError:
        return value


# --------------------------------------------------------------------------
# Parsers: each returns a list of records in the common schema
# --------------------------------------------------------------------------


def parse_ofac(path: Path, source: str) -> list[dict]:
    root = load_xml(path)
    out = []
    for e in root.iter("sdnEntry"):
        name = join(text(e, "firstName"), text(e, "lastName"))
        aliases = [join(text(a, "firstName"), text(a, "lastName")) for a in e.iter("aka")]
        identifiers, crypto = [], []
        for i in e.iter("id"):
            id_type, id_num = text(i, "idType"), text(i, "idNumber")
            if id_type.startswith("Digital Currency Address"):
                crypto.append(f"{id_type.rsplit('-', 1)[-1].strip()}: {id_num}")
            elif id_num:
                identifiers.append(join(f"{id_type.rstrip(' :-')}:", id_num, f"({text(i, 'idCountry')})" if text(i, "idCountry") else ""))
        out.append(
            record(
                source,
                list_id=text(e, "uid"),
                entity_type=text(e, "sdnType").lower(),
                name=name,
                aliases=aliases,
                programs=[text(p) for p in e.iter("program")],
                dates_of_birth=[ofac_date(text(d, "dateOfBirth")) for d in e.iter("dateOfBirthItem")],
                places_of_birth=[text(p, "placeOfBirth") for p in e.iter("placeOfBirthItem")],
                nationalities=[text(n, "country") for n in e.iter("nationality")]
                + [text(c, "country") for c in e.iter("citizenship")],
                address_countries=[text(a, "country") for a in e.iter("address")],
                identifiers=identifiers,
                crypto_addresses=crypto,
                remarks=text(e, "remarks"),
            )
        )
    return out


def parse_eu(path: Path, source: str) -> list[dict]:
    root = load_xml(path)
    out = []
    for e in root.iter("sanctionEntity"):
        names = [a.get("wholeName") or join(a.get("firstName", ""), a.get("middleName", ""), a.get("lastName", ""))
                 for a in e.findall("nameAlias")]
        names = [n for n in names if n]
        subject = (e.find("subjectType").get("code") if e.find("subjectType") is not None else "")
        remarks = " | ".join(text(r) for r in e.findall("remark") if text(r))
        out.append(
            record(
                source,
                list_id=e.get("euReferenceNumber") or e.get("logicalId", ""),
                entity_type={"person": "individual", "enterprise": "entity"}.get(subject, subject),
                name=names[0] if names else "",
                aliases=names[1:],
                programs=[r.get("programme", "") for r in e.findall("regulation")],
                dates_of_birth=[b.get("birthdate") or b.get("year", "") for b in e.findall("birthdate")],
                places_of_birth=[join(b.get("city", ""), b.get("countryDescription", "") if b.get("countryIso2Code") != "00" else "")
                                 for b in e.findall("birthdate")],
                nationalities=[c.get("countryDescription", "") for c in e.findall("citizenship")
                               if c.get("countryIso2Code") != "00"],
                address_countries=[a.get("countryDescription", "") for a in e.findall("address")
                                   if a.get("countryIso2Code") != "00"],
                identifiers=[join(f"{i.get('identificationTypeDescription', '')}:", i.get("number", ""))
                             for i in e.findall("identification") if i.get("number")],
                crypto_addresses=evm_addresses(remarks),
                listed_on=e.get("designationDate", ""),
                remarks=remarks,
            )
        )
    return out


def parse_un(path: Path, source: str) -> list[dict]:
    root = load_xml(path)
    out = []
    for kind, tag in (("individual", "INDIVIDUAL"), ("entity", "ENTITY")):
        for e in root.iter(tag):
            name = join(*(text(e, t) for t in ("FIRST_NAME", "SECOND_NAME", "THIRD_NAME", "FOURTH_NAME")))
            aliases = [text(a, "ALIAS_NAME") for a in e.iter(f"{tag}_ALIAS")] + [text(e, "NAME_ORIGINAL_SCRIPT")]
            dobs = []
            for d in e.iter("INDIVIDUAL_DATE_OF_BIRTH"):
                if text(d, "DATE"):
                    dobs.append(text(d, "DATE"))
                elif text(d, "YEAR"):
                    dobs.append(text(d, "YEAR"))
                elif text(d, "FROM_YEAR"):
                    dobs.append(f"{text(d, 'FROM_YEAR')}-{text(d, 'TO_YEAR')}")
            remarks = text(e, "COMMENTS1")
            out.append(
                record(
                    source,
                    list_id=text(e, "REFERENCE_NUMBER") or text(e, "DATAID"),
                    entity_type=kind,
                    name=name,
                    aliases=aliases,
                    programs=[text(e, "UN_LIST_TYPE")],
                    dates_of_birth=dobs,
                    places_of_birth=[join(text(p, "CITY"), text(p, "STATE_PROVINCE"), text(p, "COUNTRY"))
                                     for p in e.iter("INDIVIDUAL_PLACE_OF_BIRTH")],
                    nationalities=[text(v) for n in e.iter("NATIONALITY") for v in n.iter("VALUE")],
                    address_countries=[text(a, "COUNTRY") for a in e.iter(f"{tag}_ADDRESS")],
                    identifiers=[join(f"{text(d, 'TYPE_OF_DOCUMENT')}:", text(d, "NUMBER"))
                                 for d in e.iter("INDIVIDUAL_DOCUMENT") if text(d, "NUMBER")],
                    crypto_addresses=evm_addresses(remarks),
                    listed_on=text(e, "LISTED_ON"),
                    remarks=remarks,
                )
            )
    return out


def parse_seco(path: Path, source: str) -> list[dict]:
    root = load_xml(path)

    # sanctions-set ssid -> "Program: set description"
    programs = {}
    for prog in root.findall("sanctions-program"):
        key = next((text(k) for k in prog.findall("program-key") if k.get("lang") == "eng"), "")
        for s in prog.findall("sanctions-set"):
            if s.get("lang") == "eng":
                programs[s.get("ssid")] = key
    places, place_country = {}, {}
    for p in root.findall("place"):
        places[p.get("ssid")] = join(text(p, "location"), text(p, "area"), text(p, "country"))
        place_country[p.get("ssid")] = text(p, "country")

    def full_name(name_el: ET.Element) -> tuple[str, list[str]]:
        parts = sorted(name_el.findall("name-part"), key=lambda p: int(p.get("order", "0")))
        full = join(*(text(p, "value") for p in parts))
        # Whole-name parts carry spelling variants (e.g. Cyrillic) that are useful for matching.
        variants = [text(v) for p in parts if p.get("name-part-type") == "whole-name"
                    for v in p.findall("spelling-variant")]
        return full, variants

    out = []
    for t in root.findall("target"):
        mods = t.findall("modification")
        if any(m.get("modification-type") == "de-listed" for m in mods):
            continue  # the file keeps historical, de-listed targets
        subject = next((c for c in t if c.tag in ("individual", "entity", "object")), None)
        if subject is None:
            # Untyped target: keep it with whatever the target itself carries.
            subject, entity_type = ET.Element("none"), ""
        elif subject.tag == "object":
            entity_type = subject.get("object-type", "object")
        else:
            entity_type = subject.tag

        name, aliases, dobs, pobs, nats, countries, ids = "", [], [], [], [], [], []
        for ident in subject.findall("identity"):
            for n in ident.findall("name"):
                full, variants = full_name(n)
                if ident.get("main") == "true" and n.get("name-type") == "primary-name" and not name:
                    name = full
                else:
                    aliases.append(full)
                aliases.extend(variants)
            for d in ident.findall("day-month-year"):
                parts = [d.get("year"), d.get("month"), d.get("day")]
                dobs.append("-".join(p.zfill(2) for p in parts if p))
            pobs += [places.get(p.get("place-id"), "") for p in ident.findall("place-of-birth")]
            nats += [text(n, "country") for n in ident.findall("nationality")]
            countries += [place_country.get(a.get("place-id"), "") for a in ident.findall("address")]
            ids += [join(f"{d.get('document-type', 'ID')}:", text(d, "number"), f"({text(d, 'issuer')})" if text(d, "issuer") else "")
                    for d in ident.findall("identification-document")]
        info = [text(x) for x in subject.findall("other-information")]
        justification = [text(x) for x in subject.findall("justification")]
        listed = next((m.get("effective-date", "") for m in mods if m.get("modification-type") == "listed"), "")
        out.append(
            record(
                source,
                list_id=t.get("ssid", ""),
                entity_type=entity_type,
                name=name,
                aliases=aliases,
                programs=[programs.get(text(s), text(s)) for s in t.findall("sanctions-set-id")],
                dates_of_birth=dobs,
                places_of_birth=pobs,
                nationalities=nats,
                address_countries=countries,
                identifiers=ids,
                crypto_addresses=evm_addresses(*info),
                listed_on=listed,
                remarks=" | ".join(info + justification),
            )
        )
    return out


PARSERS = {
    "ofac_sdn": parse_ofac,
    "ofac_cons": parse_ofac,
    "eu": parse_eu,
    "un": parse_un,
    "seco": parse_seco,
}


# --------------------------------------------------------------------------
# Merge
# --------------------------------------------------------------------------


def name_key(name: str) -> str:
    """Normalise a name so 'Lastname, Firstname' and 'FIRSTNAME LASTNAME' match."""
    s = unicodedata.normalize("NFKD", name)
    s = "".join(c for c in s if not unicodedata.combining(c)).upper()
    tokens = re.findall(r"\w+", s)
    return " ".join(sorted(tokens))


def cross_reference(records: list[dict]) -> None:
    """Fill also_listed_in by matching normalised names/aliases across sources.

    This is an exact-name heuristic to help navigation, not entity resolution:
    it will miss transliteration differences and can join namesakes.
    """
    index = defaultdict(set)
    keys_per_rec = []
    for rec in records:
        keys = {name_key(n) for n in [rec["name"], *rec["aliases"]]}
        keys = {k for k in keys if len(k.replace(" ", "")) >= 6}
        keys_per_rec.append(keys)
        for k in keys:
            index[k].add(rec["source"])
    for rec, keys in zip(records, keys_per_rec):
        others = set().union(*(index[k] for k in keys)) if keys else set()
        rec["also_listed_in"] = sorted(others - {rec["source"]})


def write_outputs(records: list[dict], out_dir: Path, meta: dict) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    with open(out_dir / "sanctions_merged.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS)
        w.writeheader()
        for rec in records:
            w.writerow({k: "; ".join(v) if isinstance(v, list) else v for k, v in rec.items()})

    with open(out_dir / "sanctions_merged.json", "w", encoding="utf-8") as fh:
        json.dump({"metadata": meta, "records": records}, fh, ensure_ascii=False, indent=1)

    with open(out_dir / "crypto_addresses.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["currency", "address", "source", "jurisdiction", "list_id", "name"])
        for rec in records:
            for entry in rec["crypto_addresses"]:
                currency, _, address = entry.partition(": ")
                w.writerow([currency, address, rec["source"], rec["jurisdiction"], rec["list_id"], rec["name"]])


def main() -> int:
    here = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sources", default=",".join(SOURCES),
                    help=f"comma-separated subset of: {', '.join(SOURCES)} (default: all)")
    ap.add_argument("--data-dir", type=Path, default=here / "data",
                    help="where raw downloads are stored (default: ./data)")
    ap.add_argument("--out", type=Path, default=None,
                    help="output directory (default: one folder per run, ./output/YYYY-MM-DD_HH-MM-SS_UTC)")
    ap.add_argument("--offline", action="store_true", help="skip downloading, use the newest files in --data-dir")
    ap.add_argument("--retries", type=int, default=10, help="download attempts per source (default: 10)")
    ap.add_argument("--timeout", type=int, default=120, help="network read timeout in seconds (default: 120)")
    args = ap.parse_args()

    keys = [k.strip() for k in args.sources.split(",") if k.strip()]
    unknown = [k for k in keys if k not in SOURCES]
    if unknown:
        ap.error(f"unknown source(s): {', '.join(unknown)}")

    started = dt.datetime.now(dt.timezone.utc)
    today = dt.date.today().isoformat()  # raw downloads are grouped by local date
    if args.out is None:
        args.out = here / "output" / started.strftime(FOLDER_TIME_FMT)

    # Everything logged from here on is also written to <out>/logs.txt.
    global LOG_FILE
    args.out.mkdir(parents=True, exist_ok=True)
    LOG_FILE = open(args.out / "logs.txt", "w", encoding="utf-8")
    try:
        log(f"Run started at {started.strftime(TIME_FMT)}: {' '.join(sys.argv)}")
        return run(keys, args, today)
    except BaseException:
        LOG_FILE.write(traceback.format_exc())  # stderr gets it from the interpreter
        raise
    finally:
        log(f"Run finished at {now()}")
        LOG_FILE.close()
        LOG_FILE = None


def run(keys: list[str], args: argparse.Namespace, today: str) -> int:
    if args.offline:
        log("Offline mode: looking for previous downloads...")
        paths = latest_downloads(keys, args.data_dir)
    else:
        paths = download_all(keys, args.data_dir / today, args.retries, args.timeout)
    if not paths:
        log("Nothing to merge.")
        return 1

    records, meta = [], {"generated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "sources": {}}
    for key in keys:
        if key not in paths:
            continue
        # Only individual people go in the final list. Companies, organisations,
        # vessels and aircraft are dropped. Entries with no type are kept as "unspecified".
        recs = [r for r in PARSERS[key](paths[key], key) if r["entity_type"] in KEEP_TYPES]
        records.extend(recs)
        meta["sources"][key] = {**SOURCES[key], "file": str(paths[key]), "records": len(recs)}
        unspecified = sum(r["entity_type"] == UNSPECIFIED for r in recs)
        log(f"{tag(key)} {len(recs) - unspecified} individuals parsed from {paths[key]}"
            + (f" (plus {unspecified} entries with no type, kept as {UNSPECIFIED})" if unspecified else ""))

    cross_reference(records)
    write_outputs(records, args.out, meta)

    crypto = sum(len(r["crypto_addresses"]) for r in records)
    unspecified = sum(r["entity_type"] == UNSPECIFIED for r in records)
    log(f"\nMerged {len(records)} records from {len(meta['sources'])} source(s): "
        f"{len(records) - unspecified} individuals, {unspecified} {UNSPECIFIED}; {crypto} crypto addresses.")
    log(f"Wrote {args.out}/sanctions_merged.csv, sanctions_merged.json, crypto_addresses.csv, logs.txt")
    missing = [k for k in keys if k not in paths]
    return 2 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
