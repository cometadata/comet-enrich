# resource-type-general

Reclassify `types.resourceTypeGeneral` from free-text `resourceType` values. The method reads
DataCite records and emits an enrichment when `resourceType` suggests a more accurate
`resourceTypeGeneral` value.

This method uses no external services.

A record is corrected when:

- its `types.resourceTypeGeneral` is `Text`, `Other`, or missing/null;
- its `types.resourceType` fuzzy-matches one of the canonical DataCite `resourceTypeGeneral`
  values at or above the configured threshold;
- the matched value differs from the current one; and
- the pair is not on the redundancy-exclusion list.

The matching threshold, canonical values, typo corrections, exclusions, and input scope come from
the rules file ([`configs/reclassification_rules.yaml`](../../configs/reclassification_rules.yaml)).

## Synopsis

```text
comet-enrich resource-type-general \
  --input <DIR> --output <DIR> \
  --rules <FILE> --provenance <FILE> [OPTIONS]
```

## Options

In addition to the [global options](../usage.md#global-options):

| Option           | Default    | Description                                                                 |
|------------------|------------|-----------------------------------------------------------------------------|
| `--rules <FILE>` | _required_ | YAML rules mapping free-text `resourceType` values to `resourceTypeGeneral` |

## Example

```bash
comet-enrich resource-type-general \
  --input      /data/datacite/DataCite_Public_Data_File_2024 \
  --output     ./out \
  --rules      configs/reclassification_rules.yaml \
  --provenance configs/provenance/resource_type_general.yaml
```

Each output record uses `action: update` and `field: types`.
`originalValue` is the record's full `types` object and `enrichedValue` is the same object with
`resourceTypeGeneral` overwritten.

Run `comet-enrich resource-type-general --help` for the full option list.
