# sanction-list

Downloads the official sanctions lists that apply in the US, Switzerland, and Germany, then merges them into one file with a shared schema. Only sanctioned individuals are kept. Companies, organisations, vessels, and aircraft are dropped. Entries that the source does not classify at all are kept and labelled `unspecified`.

## Usage

Requires Python 3.9+. No packages to install.

```sh
python3 sanctions.py
```

The script downloads every list in parallel. It takes a few minutes, sometimes 5 or more, because the Swiss SECO server is slow and often drops the connection partway through. Failed downloads are retried automatically. Each run writes its results to its own subfolder, named after the UTC start time, `output/YYYY-MM-DD_HH-MM-SS_UTC/`:

| File | Contents |
|---|---|
| `sanctions_merged.csv` | One row per listed individual per source. Multi-value fields are `; `-separated |
| `sanctions_merged.json` | Same data, with multi-value fields as arrays, plus source metadata |
| `crypto_addresses.csv` | Every digital currency address attributed to a listed individual, one per row |
| `logs.txt` | Everything the script logged during the run, overwritten each time |

Raw downloads are kept in `data/YYYY-MM-DD/`.

### Options

```sh
python3 sanctions.py --sources ofac_sdn,seco   # only some lists
python3 sanctions.py --offline                 # re-merge the latest downloads without fetching
python3 sanctions.py --out /some/dir           # write there instead of output/YYYY-MM-DD_HH-MM-SS_UTC/
python3 sanctions.py --help
```

## Sources

| Key | Jurisdiction | List |
|---|---|---|
| `ofac_sdn` | US | [OFAC Specially Designated Nationals (SDN)](https://sanctionslist.ofac.treas.gov/) |
| `ofac_cons` | US | [OFAC Consolidated (non-SDN) lists](https://sanctionslist.ofac.treas.gov/) |
| `seco` | CH | [SECO Swiss consolidated sanctions list](https://www.seco.admin.ch/seco/en/home/Aussenwirtschaftspolitik_Wirtschaftliche_Zusammenarbeit/Wirtschaftsbeziehungen/exportkontrollen-und-sanktionen/sanktionen-embargos.html) |
| `eu` | EU / DE | [EU Consolidated Financial Sanctions list](https://data.europa.eu/data/datasets/consolidated-list-of-persons-groups-and-entities-subject-to-eu-financial-sanctions) |
| `un` | UN | [UN Security Council consolidated list](https://main.un.org/securitycouncil/en/content/un-sc-consolidated-list) |

**Germany** has no standalone national sanctions list. It enforces EU and UN sanctions directly, so the EU consolidated list is the list that applies in Germany. The UN list is included because it binds all three jurisdictions. EU and SECO largely mirror it.

## Output schema

`source, jurisdiction, list_id, entity_type, name, aliases, programs, dates_of_birth, places_of_birth, nationalities, address_countries, identifiers, crypto_addresses, listed_on, remarks, also_listed_in`

- `entity_type` is `individual`, or `unspecified` when the source gives no type for the entry. Companies, organisations, vessels, and aircraft are excluded before the merge. As of October 2026 every entry in every source has a type, so `unspecified` is a safeguard rather than something you will normally see.
- `also_listed_in` lists the other sources that contain the same name after normalization (accents removed, case and word order ignored). It is a navigation aid, **not** entity resolution. It misses transliteration differences and can link unrelated people who share a name.
- SECO entries that have been de-listed are excluded.

### Crypto addresses

OFAC publishes addresses as structured fields, labeled by currency (`ETH`, `XBT`, `TRX`, `USDT`, …). The EU, SECO, and UN lists mention addresses only in free text. For those lists, the script extracts `0x…` addresses and labels them `EVM`, because the text often doesn't name the chain. Non-EVM addresses in their free text, such as BTC, are not extracted. Check the `remarks` column for those.

## Disclaimer

This tool is a convenience for research and screening. It is not legal advice or a compliance program. The official publications are authoritative.
