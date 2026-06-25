# comet-enrich

`comet-enrich` is a Rust tool for producing DataCite enrichment records from the
[DataCite Public Data File](https://datafiles.datacite.org/). It reads a directory of
DataCite `*.jsonl.gz` files and writes JSONL records that conform to the DataCite
enrichment input schema
([`configs/schema/enrichment_input_schema.json`](configs/schema/enrichment_input_schema.json)).

It currently provides three enrichment methods:

- **resource-type-general**: reclassify `types.resourceTypeGeneral` from free-text `resourceType` values.
- **affiliations**: match creator affiliation strings to ROR IDs.
- **funders**: match funder names to ROR IDs.

## Requirements

- **Rust 1.85+ (edition 2024)**: install via [rustup](https://rustup.rs/)
  or the [Rust install page](https://www.rust-lang.org/tools/install).
- **Marple match service**: only for the ROR-matching methods (`affiliations`, `funders`).
  Run COMET's branch:

  ```bash
  git clone -b feature/marple-speed-improvements https://gitlab.com/jdiprose/marple.git
  ```

  then follow that repository's README to build and run it.
- **ROR registry dataset**: for `affiliations` and `funders` only. This is used during
  reconciliation. Download the ROR data dump from [its DOI](https://doi.org/10.5281/zenodo.6347574),
  extract the zip, and point `--ror-file` at the JSON file inside.

## Data sources

- **DataCite Public Data File**: <https://datafiles.datacite.org/>
- **ROR**: <https://doi.org/10.5281/zenodo.6347574>

## Documentation

- [Installation](docs/installation.md): prerequisites, building, and tests.
- [Usage](docs/usage.md): how to run the CLI.
- Commands:
  - [resource-type-general](docs/commands/resource-type-general.md)
  - [affiliations](docs/commands/affiliations.md)
  - [funders](docs/commands/funders.md)

## Acknowledgements

This project includes code ported and adapted from the following COMET projects by
[@adambuttrick](https://github.com/adambuttrick):

- [`reclassify-resource-type-general`](https://github.com/cometadata/reclassify-resource-type-general)
- [`match-datacite-affiliations-to-ror-ids`](https://github.com/cometadata/match-datacite-affiliations-to-ror-ids)
- [`match-datacite-funders-to-ror-ids`](https://github.com/cometadata/match-datacite-funders-to-ror-ids)
