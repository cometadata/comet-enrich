# Usage

`comet-enrich` runs a single enrichment method at a time:

```text
comet-enrich <method> [OPTIONS]
```

The available methods are [`resource-type-general`](commands/resource-type-general.md),
[`affiliations`](commands/affiliations.md), and [`funders`](commands/funders.md). The
[`diff`](commands/diff.md) command compares two completed runs into a diff release.

Use `--help` on the binary or a method to see the available options:

```bash
comet-enrich --help
comet-enrich resource-type-general --help
comet-enrich affiliations --help
```

## Quick start

Reclassify resource types for the DataCite Public Data File:

```bash
comet-enrich resource-type-general \
  --input      /data/datacite \
  --output     ./out \
  --rules      configs/reclassification_rules.yaml \
  --source-id  10.1234/example
```

## Input data

Point `--input` at a directory of DataCite `*.jsonl.gz` files, such as the extracted
[DataCite Public Data File](https://datafiles.datacite.org/). Subdirectories are searched
automatically. Keep the input files unchanged during the run.

Records are automatically de-duplicated by DOI, keeping the version with the latest
`attributes.updated` timestamp. If a duplicated DOI has a missing or invalid timestamp,
the run stops. See [architecture.md](architecture.md#doi-deduplication) for details.

## Output and validation

Each method writes gzip-compressed JSONL parts under `--output/enrichments/`, one record per line.
Records are validated as they are written. Invalid records are diverted to
`enrichments.failed.jsonl` with the validator error attached, and the run continues. A
transform method refuses an `--output` that contains `.work` from a staged run; use a separate
directory per method.

Part files are storage chunks, not semantic partitions. Consumers should read every
`*.jsonl.gz` file under `enrichments/`.

Use these options to change the validation behaviour:

- `--schema <FILE>`: validate against a custom JSON Schema instead of the built-in one.
- `--no-validate`: skip validation entirely.

### Partial runs

A partial run is one where an input file failed to read, a source line could not be parsed, a
record failed schema validation, a lookup was lost to a timeout or error, no enrichment records
were emitted, or the staged pipeline did not complete. Its manifest reports
`exit_status: partial` and the command exits non-zero. The output is retained for debugging,
but should not be published and cannot be used with `comet-enrich diff`.

A standalone `--stage extract` or `--stage query` writes no manifest and exits 0 when the stage
completes without errors.

## Source ID

Every enrichment record carries a `sourceId` identifying the enrichment project that
produced it. Provide it with `--source-id`, using a DOI name such as `10.1234/example`.
ASCII letters are stored in lowercase.

## Global options

These flags are shared by every method:

| Option                         | Default    | Description                                                                                |
|--------------------------------|------------|--------------------------------------------------------------------------------------------|
| `-i, --input <DIR>`            | _required_ | Input directory of DataCite `*.jsonl.gz` files, searched recursively                       |
| `-o, --output <DIR>`           | _required_ | Output directory; writes `enrichments/part_NNNN.jsonl.gz` (and `enrichments.failed.jsonl`) |
| `--source-id <ID>`             | _required_ | DOI name like `10.1234/example`; ASCII case-insensitive, written lowercase to `sourceId`   |
| `-t, --threads <N>`            | `0`        | Worker threads; `0` uses all available CPUs                                                |
| `-b, --batch-size <N>`         | `5000`     | Enrichment records per internal batch                                                      |
| `--output-part-size-mib <MIB>` | `256`      | Target compressed MiB per final enrichment part                                            |
| `--output-writer-lanes <N>`    | `1`        | Parallel writer lanes for final enrichment output                                          |
| `--schema <FILE>`              | built-in   | Validate output against a custom JSON Schema                                               |
| `--no-validate`                | off        | Skip output schema validation                                                              |
| `--log-level <LEVEL>`          | `info`     | Minimum log level (`trace`, `debug`, `info`, `warn`, `error`)                              |

Each method adds its own options. See its page below.

## Commands

| Method                                                       | What it does                                                                |
|--------------------------------------------------------------|-----------------------------------------------------------------------------|
| [`resource-type-general`](commands/resource-type-general.md) | Reclassify `types.resourceTypeGeneral` from free-text `resourceType` values |
| [`affiliations`](commands/affiliations.md)                   | Match creator and contributor affiliation strings to ROR IDs                |
| [`funders`](commands/funders.md)                             | Match funder names to ROR IDs                                               |
| [`diff`](commands/diff.md)                                   | Diff two completed runs into asserted/retracted/superseded events           |
