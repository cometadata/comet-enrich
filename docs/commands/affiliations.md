# affiliations

Extract unique creator and contributor affiliation strings from the DataCite data, match them to
ROR IDs via the [Marple](https://gitlab.com/jdiprose/marple) match service, and write the matches
back to creator and contributor records as enrichment output.

The method runs as a three-stage pipeline:

1. **extract**: scan the corpus and collect the unique affiliation strings to look up.
2. **query**: resolve those strings with the match service, using Marple's `affiliation` task.
3. **reconcile**: join the matches back to the records and emit enrichment records.

Running `affiliations` without a stage runs the whole pipeline. Intermediate files are written to a
`.work` directory inside `--output`. A later run resumes from completed stages there unless
`--from-scratch` is given.

## Prerequisites

- A running **Marple** match service, loaded with ROR data, that matches affiliation strings to
  ROR IDs (see the [Requirements](../../README.md#requirements)). The matched ROR id comes straight
  from the match service, so no ROR registry file is needed.

## Synopsis

```text
comet-enrich affiliations \
  --input <DIR> --output <DIR> \
  --provenance <FILE> \
  [OPTIONS] [extract|query|reconcile]
```

## Options

In addition to the [global options](../usage.md#global-options):

| Option                    | Default                 | Description                                                                                |
|---------------------------|-------------------------|--------------------------------------------------------------------------------------------|
| `--ror-service-url <URL>` | `http://localhost:8000` | Base URL of the ROR match service / Marple                                                 |
| `--ror-batch-size <N>`    | `50`                    | Inputs per ROR match-service bulk request                                                  |
| `--ror-concurrency <N>`   | `50`                    | Concurrent ROR match-service requests                                                      |
| `--ror-timeout <SECS>`    | `30`                    | ROR match-service request timeout in seconds                                               |
| `--hash-bits <N>`         | `64`                    | Dedup hash width (`64` or `128`)                                                          |
| `--from-scratch`          | off                     | Ignore existing stage outputs in `.work` and rerun all stages                             |

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
  --output     ./out \
  --provenance configs/provenance/affiliations.yaml \
  --ror-service-url http://localhost:8000 \
  --threads    16
```

Run `comet-enrich affiliations --help` for the full option list.
