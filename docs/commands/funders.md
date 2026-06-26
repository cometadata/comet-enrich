# funders

Extract unique funder names from the DataCite data, match them to ROR IDs via the
[Marple](https://gitlab.com/jdiprose/marple) match service, and write the matches back to funding
references as enrichment output.

The method runs as a three-stage pipeline:

1. **extract**: scan the corpus and collect the unique funder names to look up.
2. **query**: resolve those names with the match service, using Marple's `funder` task.
3. **reconcile**: join the matches back to the records, look up matched ROR IDs in the ROR registry
   dataset, and emit enrichment records. This also uses Crossref Funder ID–to–ROR crosswalks from
   the registry data.

Running `funders` without a stage runs the whole pipeline. Intermediate files are written to a
`.work` directory inside `--output`. A later run resumes from completed stages there unless
`--from-scratch` is given.

## Prerequisites

- A running **Marple** match service, loaded with ROR data, that matches funder names to
  ROR IDs (see the [Requirements](../../README.md#requirements)).
- The **ROR registry dataset** (`--ror-file`): the JSON extracted from the ROR data dump, used at
  the reconcile stage to resolve matched ROR IDs.

## Synopsis

```text
comet-enrich funders \
  --input <DIR> --output <DIR> \
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
| `--from-scratch`          | off                     | Ignore existing stage outputs in `.work` and rerun all stages                             |

## Stages

Run a single stage by naming it after the method:

```bash
comet-enrich funders extract   ...   # collect the unique funder names
comet-enrich funders query     ...   # match them against Marple
comet-enrich funders reconcile ...   # emit the enrichment records
```

Omit the stage to run all three in order.

## Full pipeline example

```bash
comet-enrich funders \
  --input      /data/datacite/DataCite_Public_Data_File_2024 \
  --output     ./out \
  --provenance configs/provenance/funders.yaml \
  --ror-file   /data/ror/v2.6-2026-04-14-ror-data.json \
  --ror-service-url http://localhost:8000 \
  --threads    16
```

Run `comet-enrich funders --help` for the full option list.
