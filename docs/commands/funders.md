# funders

Extract unique funder names from the DataCite data, match them to ROR IDs via the
[Marple](https://gitlab.com/jdiprose/marple) match service, and write the matches back to funding
references as enrichment output.

The method runs as a three-stage pipeline:

1. **extract**: scan the corpus and collect the unique funder names to look up.
2. **query**: resolve those names with the match service, using Marple's `funder` task.
3. **reconcile**: join matches back to the records and emit enrichment records. Valid ROR IDs and
   crosswalk-mapped Crossref Funder IDs remain unchanged; all other matched funding
   references, including those with invalid ROR IDs or non-ROR identifiers, receive the matched ROR
   ID.

Running `funders` without a stage runs the whole pipeline. Intermediate files are written to a
`.work` directory inside `--output`. A later run resumes from completed stages there unless
`--from-scratch` is given.

## Prerequisites

- A running **Marple** match service, loaded with ROR data, that matches funder names to
  ROR IDs (see the [Requirements](../../README.md#requirements)).
- The **ROR registry dataset** (`--ror-file`): the JSON extracted from the ROR data dump, used at
  reconcile to skip references already identified by their Crossref Funder ID.

## Synopsis

```text
comet-enrich funders \
  --input <DIR> --output <DIR> \
  --source-id <ID> --ror-file <FILE> \
  [OPTIONS] [--stage <extract|query|reconcile>]
```

## Options

In addition to the [global options](../usage.md#global-options):

| Option                    | Default                 | Description                                                                                |
|---------------------------|-------------------------|--------------------------------------------------------------------------------------------|
| `--ror-service-url <URL>` | `http://localhost:8000` | Base URL of the ROR match service / Marple                                                 |
| `--ror-file <FILE>`       | _required_              | ROR registry JSON for Crossref Funder ID checks                                             |
| `--ror-batch-size <N>`    | `50`                    | Inputs per ROR match-service bulk request                                                  |
| `--ror-concurrency <N>`   | `50`                    | Concurrent ROR match-service requests                                                      |
| `--ror-timeout <SECS>`    | `30`                    | ROR match-service request timeout in seconds                                               |
| `--hash-bits <N>`         | `64`                    | Dedup hash width (`64` or `128`)                                                          |
| `--from-scratch`          | off                     | Ignore existing stage outputs in `.work` and rerun all stages                             |
| `--stage <STAGE>`         | all stages              | Run a single stage: `extract`, `query`, or `reconcile`                                    |

## Stages

Run a single stage with `--stage`:

```bash
comet-enrich funders ... --stage extract     # collect the unique funder names
comet-enrich funders ... --stage query       # match them against Marple
comet-enrich funders ... --stage reconcile   # emit the enrichment records
```

Omit `--stage` to run all three in order.

## Full pipeline example

```bash
comet-enrich funders \
  --input      /data/datacite/DataCite_Public_Data_File_2024 \
  --output     ./out \
  --source-id  10.1234/example \
  --ror-file   /data/ror/v2.6-2026-04-14-ror-data.json \
  --ror-service-url http://localhost:8000 \
  --threads    16
```

Run `comet-enrich funders --help` for the full option list.
