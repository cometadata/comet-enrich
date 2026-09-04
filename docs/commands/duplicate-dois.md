# duplicate-dois

Find repeated DOIs within one source snapshot, including which source month contains each
occurrence and which occurrence enrichment would keep. This utility is part of the
`tools` binary. It reports duplicates without modifying source records.

## Example

```bash
cargo build --release -p comet-enrich-tools
target/release/tools duplicate-dois \
  data/datacite_ingest/scheduled__2026-09-03T00:00:00+00:00 \
  --output duplicate-dois.json
```

## Input

Pass one snapshot containing `updated_YYYY-MM` directories. The utility reads all
`*.jsonl.gz` parts beneath those directories. It matches exact DOI strings, using `id`
when it is a non-blank string and otherwise falling back to `attributes.doi`.

## Options

| Option                | Default    | Description                                  |
|-----------------------|------------|----------------------------------------------|
| `--output <FILE>`     | _required_ | File to write the JSON report to             |
| `--threads <N>`       | `0`        | Worker threads; `0` uses all available CPUs  |
| `--log-level <LEVEL>` | `info`     | Minimum log level                            |

## Report

The JSON report includes individual DOI details. The terminal shows summary counts and
a per-month table:

```text
Scanned records: 81234567
Records without DOI: 12
Malformed lines: 0
Distinct DOIs: 81227555
Duplicated DOIs: 7000
Extra occurrences: 7000

Duplicated DOIs by month:
  updated_2026-08  4102
  updated_2026-07  3877
  updated_2026-03  21
Wrote duplicate-dois.json
```

The month table counts distinct duplicated DOIs with at least one occurrence in that
month directory, so a DOI repeated across two months counts once in each.

The JSON file has three parts. `summary` holds the counts printed to the terminal.
`by_month` is the month table keyed by directory name. `dois` lists each duplicated DOI in
sorted order with every occurrence: the month directory, file path relative to the snapshot,
line number (starting at one), and `updated`, containing the record's `attributes.updated`
timestamp when valid (`null` when missing or invalid). The folder names identify source
months; these are reported separately from the record's update timestamp.

`selected` is `true` for the occurrence enrichment would keep. See
[DOI deduplication](../architecture.md#doi-deduplication) for the selection rules.
If a duplicated DOI has a missing or invalid update timestamp, none of its occurrences are
selected. The report is still written, but running enrichment on the same data would fail.

```json
{
  "summary": {
    "scanned_records": 5,
    "records_without_doi": 1,
    "malformed_lines": 0,
    "distinct_dois": 3,
    "duplicated_dois": 1,
    "extra_occurrences": 1
  },
  "by_month": {
    "updated_2026-07": 1,
    "updated_2026-08": 1
  },
  "dois": [
    {
      "doi": "10.example/item",
      "occurrences": 2,
      "locations": [
        {
          "month": "updated_2026-07",
          "path": "updated_2026-07/part_0012.jsonl.gz",
          "line": 842,
          "updated": "2026-07-18T00:00:00Z",
          "selected": false
        },
        {
          "month": "updated_2026-08",
          "path": "updated_2026-08/part_0003.jsonl.gz",
          "line": 219,
          "updated": "2026-08-02T00:00:00Z",
          "selected": true
        }
      ]
    }
  ]
}
```

Malformed records are counted and skipped. An unreadable file stops the scan before the
report is written.
