# affiliations

Extract unique creator affiliation strings from the DataCite data, match them to ROR IDs via the
[Marple](https://gitlab.com/jdiprose/marple) match service, and write the matches back to creator
records as enrichment output.

The method runs as a three-stage pipeline:

1. **extract**: scan the corpus and collect the unique affiliation strings to look up.
2. **query**: resolve those strings with the match service, using Marple's `affiliation` task.
3. **reconcile**: join the matches back to the records, look up matched ROR IDs in the ROR registry
   dataset, and emit enrichment records.

Running `affiliations` without a stage runs the whole pipeline. Intermediate files are written to
`--work-dir`. A later run resumes from completed stages in that directory unless `--from-scratch`
is given.

## Prerequisites

- A running **Marple** match service, loaded with ROR data, that matches affiliation strings to
  ROR IDs (see the [Requirements](../../README.md#requirements)).
- The **ROR registry dataset** (`--ror-file`): the JSON extracted from the ROR data dump, used at
  the reconcile stage to resolve matched ROR IDs.

## Synopsis

```text
comet-enrich affiliations \
  --input <DIR> --output <FILE> \
  --provenance <FILE> --ror-file <FILE> \
  [OPTIONS] [extract|query|reconcile]
```

## Options

In addition to the [global options](../usage.md#global-options):

| Option                    | Default                 | Description                                                                                |
|---------------------------|-------------------------|--------------------------------------------------------------------------------------------|
| `--ror-service-url <URL>` | `http://localhost:8000` | Base URL of the ROR match service / Marple                                                 |
| `--ror-file <FILE>`       | _required_              | ROR registry dataset used to reconcile matched ROR IDs                                        |
| `--ror-batch-size <N>`    | `50`                    | Inputs per ROR match-service bulk request                                                  |
| `--ror-concurrency <N>`   | `50`                    | Concurrent ROR match-service requests                                                      |
| `--ror-timeout <SECS>`    | `30`                    | ROR match-service request timeout in seconds                                               |
| `--work-dir <DIR>`        | _optional_              | Directory for extracted inputs and match results; use a stable path to resume runs |
| `--from-scratch`          | off                     | Ignore existing work-dir artifacts and rerun all stages                                    |

## Stages

Run a single stage by naming it after the method:

```bash
comet-enrich affiliations extract   ...   # collect the unique affiliations
comet-enrich affiliations query     ...   # match them against Marple
comet-enrich affiliations reconcile ...   # emit the enrichment records
```

Omit the stage to run all three in order.

## Full pipeline example

```bash
comet-enrich affiliations \
  --input      /data/datacite/DataCite_Public_Data_File_2024 \
  --output     affiliations.jsonl \
  --provenance configs/provenance/affiliations.yaml \
  --ror-file   /data/ror/v2.3-2026-02-24-ror-data.json \
  --ror-service-url http://localhost:8000 \
  --work-dir   /work/affiliations \
  --threads    16
```

Run `comet-enrich affiliations --help` for the full option list.
