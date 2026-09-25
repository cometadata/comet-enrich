# diff

`comet-enrich diff` compares two successful runs of the same method and writes events
describing which enrichments were added, removed, or changed.

Input requirements:

- Both directories contain `enrichments/` and a `manifest.json` reporting `exit_status: success`.
- Both releases and any reused stages (`report.stage_versions`) were written by comet-enrich
  0.4.0 or later. Versions may differ.
- Each input's record count matches its manifest's `report.counters.emitted`, preventing
  missing data from being interpreted as retractions.

Older releases lack content keys and require a `--from-scratch` rerun before they can be diffed.

Use separate directories for `--old`, `--new`, and `--output`; none may contain another.
Existing enrichment output and the manifest under `--output` are replaced. `--output` must not
contain `.work` from a staged run.

## Synopsis

```text
comet-enrich diff --old <DIR> --new <DIR> --output <DIR> [OPTIONS]
```

## Example

```bash
comet-enrich diff \
  --old    ./runs/2026-01-02/full \
  --new    ./runs/2026-02-02/full \
  --output ./runs/2026-02-02/diff
```

## Options

| Option                          | Default    | Description                                                        |
|---------------------------------|------------|--------------------------------------------------------------------|
| `--old <DIR>`                   | _required_ | Previous run's output directory (`enrichments/` + `manifest.json`) |
| `--new <DIR>`                   | _required_ | Current run's output directory (`enrichments/` + `manifest.json`)  |
| `-o, --output <DIR>`            | _required_ | Output directory for the diff release                              |
| `--output-part-size-mib <MIB>`  | `256`      | Target compressed size for each output part                        |
| `--output-writer-lanes <N>`     | `1`        | Number of output writer lanes                                      |
| `--log-level <LEVEL>`           | `info`     | Minimum log level                                                  |

Run `comet-enrich diff --help` for the full option list.

## Output

The diff matches enrichments by their content key (`contentKey`) and writes an event when an
enrichment is added, removed, or its enriched value changes:

| Event        | Meaning                                      | Consumer action        |
|--------------|----------------------------------------------|------------------------|
| `asserted`   | Content key present now, absent before.      | Apply the enrichment.  |
| `retracted`  | Content key present before, absent now.      | Remove the enrichment. |
| `superseded` | Same content key, different `enrichedValue`. | Replace it in place.   |

Unchanged enrichments produce no events. Changes to `sourceId` alone do not produce events.

For updates, changing only `enrichedValue` preserves the content key and produces a
`superseded` event. Inserts use `enrichedValue` to derive their content key, so changing
an inserted value produces a `retracted` event for the old content key and an `asserted`
event for the new content key.

```text
<output>/
  enrichments/
    part_0000.jsonl.gz
  manifest.json
```

Each event line is an enrichment record plus an `event` field. `asserted` and `superseded`
lines carry the new record; `retracted` lines carry the old record. This example uses the
`resource-type-general` method:

```json
{"doi":"10.1/x","action":"update","field":"types","originalValue":{"resourceTypeGeneral":"Text"},"enrichedValue":{"resourceTypeGeneral":"Dataset"},"sourceId":"10.1234/example","contentKey":"1dc558dae21181dcb1bff9c1c744244f","event":"asserted"}
```

`manifest.json` records the compared releases, event counts, and elapsed time. It is written
only when the diff succeeds. If no enrichments have changed, the diff succeeds with a
manifest and no part files.
