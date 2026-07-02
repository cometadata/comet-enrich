# Usage

`comet-enrich` runs a single enrichment method at a time:

```text
comet-enrich <method> [OPTIONS]
```

The available methods are [`resource-type-general`](commands/resource-type-general.md),
[`affiliations`](commands/affiliations.md), and [`funders`](commands/funders.md).

Use `--help` on the binary, a method, or a method stage to see the available options:

```bash
comet-enrich --help
comet-enrich resource-type-general --help
comet-enrich affiliations query --help
```

## Quick start

Reclassify resource types for the DataCite Public Data File:

```bash
comet-enrich resource-type-general \
  --input      /data/datacite \
  --output     ./out \
  --rules      configs/reclassification_rules.yaml \
  --provenance configs/provenance/resource_type_general.yaml
```

## Input data

Point `--input` at a directory of DataCite `*.jsonl.gz` files, such as the extracted
[DataCite Public Data File](https://datafiles.datacite.org/). The input directory is searched
recursively.

## Output and validation

Each method writes gzip-compressed JSONL parts under `--output/enrichments/`, one record per line.
Records are validated as they are written. Invalid records are diverted to
`enrichments.failed.jsonl` with the validator error attached, and the run continues.

Part files are storage chunks, not semantic partitions. Consumers should read every
`*.jsonl.gz` file under `enrichments/`.

Use these options to change the validation behaviour:

- `--schema <FILE>`: validate against a custom JSON Schema instead of the built-in one.
- `--no-validate`: skip validation entirely.

## Provenance

Every enrichment record includes a provenance block (`contributors` and `resources`) loaded from
`--provenance <FILE>`. Example files live in [`configs/provenance/`](../configs/provenance), with
one file per method:

- `resource_type_general.yaml`
- `affiliations.yaml`
- `funders.yaml`

The provenance file is validated before the method runs.

## Global options

These flags are shared by every method:

| Option                         | Default    | Description                                                          |
|--------------------------------|------------|----------------------------------------------------------------------|
| `-i, --input <DIR>`            | _required_ | Input directory of DataCite `*.jsonl.gz` files, searched recursively |
| `-o, --output <DIR>`           | _required_ | Output directory; writes `enrichments/part_NNNN.jsonl.gz` (and `enrichments.failed.jsonl`) |
| `--provenance <FILE>`          | _required_ | YAML provenance metadata attached to each record                     |
| `-t, --threads <N>`            | `0`        | Worker threads; `0` uses all available CPUs                          |
| `-b, --batch-size <N>`         | `5000`     | Enrichment records per internal batch                                |
| `--output-part-size-mib <MIB>` | `256`      | Target compressed MiB per final enrichment part                      |
| `--output-writer-lanes <N>`    | `1`        | Parallel writer lanes for final enrichment output                    |
| `--schema <FILE>`              | built-in   | Validate output against a custom JSON Schema                         |
| `--no-validate`                | off        | Skip output schema validation                                        |
| `--log-level <LEVEL>`          | `info`     | Minimum log level (`trace`, `debug`, `info`, `warn`, `error`)        |

Each method adds its own options. See its page below.

## Commands

| Method                                                       | What it does                                                                |
|--------------------------------------------------------------|-----------------------------------------------------------------------------|
| [`resource-type-general`](commands/resource-type-general.md) | Reclassify `types.resourceTypeGeneral` from free-text `resourceType` values |
| [`affiliations`](commands/affiliations.md)                   | Match creator and contributor affiliation strings to ROR IDs                |
| [`funders`](commands/funders.md)                             | Match funder names to ROR IDs                                               |
