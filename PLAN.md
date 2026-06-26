# comet-enrich: lookup-method framework and ROR method ports

## Context

comet-enrich is the generation side of COMET's procedural enrichment: a Rust tool that reads the
DataCite data file and emits enrichment records conforming to `enrichment_input_schema.json`. The
resource-type reclassifier (a pure transform) is already ported. The two ROR lookup methods,
affiliation matching and funder matching, are still stubs.

This plan covers building the shared lookup machinery the architecture notes
(`.claude/COMET Procedural Enrichment Architecture Notes.md`) describe but which does not exist
yet, then porting the two lookup methods on top of it. The architecture notes are the reference
design; this plan records where we follow them, where we deliberately differ, and what we are not
building. The order is deliberately incremental: a large amount of core work lands before the
first method port.

Reference designs:

- `.claude/COMET Procedural Enrichment Architecture Notes.md` (the spec).
- `.claude/DIFF.md` (changefile/diff design, a later track, not built here).
- Prototype repos being ported (local checkouts, siblings of this repo):
  `../reclassify-resource-type-general`,
  `../match-datacite-affiliations-to-ror-ids` (on the `feature/marple` branch, which carries the
  bulk `match/bulk` client we standardise on),
  `../match-datacite-funders-to-ror-ids`.

## 1. Implemented from the spec, staying the same

These already exist in this repo and the plan keeps them:

- Parallel reader for `*.jsonl.gz` data-file inputs (rayon fan-out, glob) in
  `crates/core/src/reader.rs`.
- The `EnrichmentMethod` trait with `extract` / `lookup` / `map_back`, where `lookup` has a default
  no-op so pure transforms ignore it (`crates/core/src/method.rs`). We keep the single-trait shape;
  we are not splitting it into a separate `LookupMethod` extension. Two small, backward-compatible
  additions are described in 4a.3.
- The enrichment-record builder and static provenance template handling
  (`crates/core/src/provenance.rs`: `build_enrichment_record`, `load_template`,
  `EnrichmentTemplate`).
- The canonical schema as the single source of truth (`configs/schema/enrichment_input_schema.json`)
  plus the JSON-Schema validator (`crates/core/src/schema.rs`).
- The streaming JSONL writer that validates each record at the write boundary
  (`crates/core/src/writer.rs`). Behaviour changes in Stage 1 and Stage 2.
- The resource-type-general method, fully ported, including its end-to-end test
  (`crates/datacite-resource-type-general/`).
- Structured logging and run counters (`RunStats`), extended into `report.json` in Stage 3.
- Stage-planning scaffolding (`crates/core/src/staged.rs`: `Stage`, `WorkDir`, `stages_to_run`,
  `LookupConfig`), which is currently unused and gets driven by the staged runner in Stage 6.

## 2. Comparisons

### 2a. Spec on-disk contract vs prototype files

| Spec contract file | Prototype equivalent (funders / affiliations) | Decision |
|---|---|---|
| `extractions.jsonl` | `doi_funders.jsonl` / `doi_author_affiliations.jsonl` | Rename to `extractions.jsonl`, one serialized extraction per line. |
| dedup input list | `unique_funder_names.json` / `unique_affiliations.json` | Standardise to `inputs.jsonl`, one row per unique input keyed by hash, in the work area. |
| `lookups.jsonl` | `ror_matches.jsonl` | Rename. |
| `lookups.failed.jsonl` | `ror_matches.failed.jsonl` | Rename. |
| `lookups.checkpoint` | `ror_matches.checkpoint` | Rename. |
| `enrichments.jsonl` | `enrichments.jsonl` (enrichment format) / `enriched_records.jsonl` (default) | Always the enrichment format, schema valid. Becomes a directory of compressed parts (Stage 2). |
| `enrichments.failed.jsonl` | none | New. Schema-validation failures diverted here with the validator error (Stage 1). |
| not in spec contract | `existing_assignments.jsonl`, `existing_assignments_aggregated.jsonl`, `disagreements.jsonl` | Dropped. We follow the spec and do not produce these. The exclusion logic that affects the enrichment output is still kept (see 4a.7 and 4a.8). |
| `manifest.json` | none | New (Stage 3). |
| `report.json` | stderr summary only | New, structured (Stage 3). |

### 2b. Output-file inventory across the three prototypes

| Role | reclassify | affiliations | funders |
|---|---|---|---|
| Final enrichment output | `--output` file (enrichment format) | `enrichments.jsonl` / `enriched_records.jsonl` | `enrichments.jsonl` / `enriched_records.jsonl` |
| Per-occurrence ledger | none (single pass) | `doi_author_affiliations.jsonl` | `doi_funders.jsonl` |
| Unique inputs | none | `unique_affiliations.json` | `unique_funder_names.json` |
| Match results / failures / resume | none | `ror_matches.jsonl` / `.failed.jsonl` / `.checkpoint` | `ror_matches.jsonl` / `.failed.jsonl` / `.checkpoint` |
| Existing assignments + aggregated | none | two files | two files (with `resolution_source`) |
| Conflicts | none | `disagreements.jsonl` | `disagreements.jsonl` |
| Reporting | stderr counts | stderr summary | stderr summary |

The existing-assignment and disagreement files are not carried over. Per-method differences we do
preserve: funders resolves a Crossref Funder ID to ROR crosswalk and uses it to exclude already
identified references from enrichment; affiliations has no crosswalk and tracks two record fields,
creators and contributors. The funder client was single GET in the prototype; we move it to the
bulk endpoint to match affiliations.

## 3. Target file layout

The Airflow output directory is the run directory. comet-enrich writes its artifacts inside it.
Scratch always lives in a `.work` subfolder of the output directory, which Airflow excludes from
the S3 upload with an s5cmd exclude rule (for example `--exclude ".work/*"`).

```
<output>/                         uploaded by s5cmd
  enrichments/
    part_0000.jsonl.gz
    part_0001.jsonl.gz
    ...
  enrichments.failed.jsonl        schema-validation failures (small)
  report.json
  manifest.json
  .work/                          scratch, excluded from upload
    extractions/                  per-occurrence ledger, one part per input file
    inputs.jsonl                  unique inputs keyed by hash
    lookups.jsonl
    lookups.failed.jsonl
    lookups.checkpoint
    extract.done query.done reconcile.done   stage markers
```

Notes:

- `--output` changes from a file to a directory. This is a breaking CLI change to coordinate with
  the `comet-data-infrastructure` DAG that invokes `comet-enrich <method>`.
- The work area is always `<output>/.work`; there is no separate work-dir option.
- The reclassifier (transform, no lookup) produces only `enrichments/`,
  `enrichments.failed.jsonl`, `report.json`, and `manifest.json`. It has no `.work` lookup files.
- Output is split into compressed parts but is order agnostic: record order across parts is not
  stable run to run, which is acceptable. Diffing (a later track) sorts a manifest, not the data.

## 4. Features to implement

### 4a. Same as the spec

- xxh3 content-addressed dedup store in core, with a shared input-hash helper.
- The resumable match-service client lifted into core, behind a `MatchService` trait.
- The staged runner that drives extract, query, reconcile.
- The on-disk stage contract files.
- A standardised `report.json` produced in core.
- A run `manifest.json` produced in core.
- The affiliation and funder methods.

#### 4a.1 xxh3 dedup store and input hashing

New module `crates/core/src/hash.rs` (or `dedup.rs`), re-exported from `lib.rs`. Add `xxhash-rust`
with the `xxh3` feature to the workspace dependencies and to core.

- `pub fn hash_input(s: &str) -> String { format!("{:016x}", xxh3_64(s.as_bytes())) }`. This is the
  same 16-hex xxh3-64 the prototypes use (`hash_funder_name` / `hash_affiliation`), so persisted
  hashes match. Note: `DIFF.md` calls for xxh3-128 for the future diff content-hash; that is a
  separate concern and does not change this dedup hash.
- The dedup itself is a `BTreeSet<String>` (ordered for deterministic `inputs.jsonl`) accumulated
  during the extract stage. Provide a small helper that takes extracted inputs, dedups, and writes
  `inputs.jsonl` rows of `{ "hash": <hash>, "value": <input> }`.

Used by the extract stage (4a.3) and by each method via `inputs` (4a.7, 4a.8).

#### 4a.2 Match-service client behind a trait

New module `crates/core/src/match_service/` (`mod.rs`, `client.rs`, `checkpoint.rs`). Add `tokio`,
`reqwest` (json feature), `urlencoding`, and `async-trait` to core; add `wiremock` as a dev
dependency. Lift the client almost verbatim from
`../match-datacite-affiliations-to-ror-ids/src/query/client.rs`.

- Trait:
  ```text
  #[async_trait::async_trait]
  pub trait MatchService: Sync {
      /// Resolve one batch. One result slot per input, in input order:
      /// Some((id, confidence)) on a match (first item wins), None for no match.
      async fn match_bulk(&self, inputs: &[String], task: &str)
          -> anyhow::Result<Vec<Option<(String, f64)>>>;
  }
  ```
- `MarpleClient` implements it: `POST {base_url}/match/bulk?task={task}` with body
  `{ "inputs": [...] }`; parse `{ message: { items: [ { items: [ { id, confidence } ] } ] } }`,
  taking the first inner item per input; assert the response length equals the input length; retry
  twice with backoff; honour `Retry-After` on 429; return a clear error on 413 (batch too large);
  configurable timeout. Constructed from the lookup config (`base_url`, `timeout`).
- `FakeMatchService` behind a `test-support` feature: holds a
  `HashMap<String, (String, f64)>` and returns matches per input. Used to unit-test the staged
  runner without HTTP. The real client's HTTP resilience is tested with wiremock.
- `Checkpoint` (in `checkpoint.rs`, lifted from the prototype): a set of processed hashes with
  `load`, `save`, `is_processed`, `mark_processed`, persisted to `lookups.checkpoint`.

#### 4a.3 Staged runner and trait integration

New module `crates/core/src/staged_run.rs` (alongside the existing `staged.rs`), exported from
`lib.rs`. Entry point roughly:

```text
pub fn run_staged<M>(
    method: &M,
    io: &StagedIo,                 // input dir, output dir (the run dir)
    cfg: &LookupConfig,            // service url, batch size, concurrency, timeout, from_scratch
    svc: &dyn MatchService,
    template: &EnrichmentTemplate,
    validator: Option<&JSONSchema>,
    only_stage: Option<Stage>,
) -> anyhow::Result<Report>
where
    M: EnrichmentMethod,
    M::Extraction: serde::Serialize + serde::de::DeserializeOwned,
    M::Lookup: serde::Serialize + serde::de::DeserializeOwned,
```

Two small additions to `EnrichmentMethod` are needed. These are the recommended options for
decisions 8.1 and 8.2; see §8 for the alternatives and trade-offs before implementing:

- `fn inputs(&self, _extraction: &Self::Extraction) -> Vec<String> { Vec::new() }` returns the
  unique lookup inputs contributed by one extraction. Transform methods keep the default
  (decision 8.1).
- `EnrichmentParts` gains a per-record `field`, and `build_enrichment_record` uses it (the runner
  passes `parts.field`). This lets affiliations emit `creators` and `contributors` per record.
  resource-type-general sets `"types"`; funders sets `"fundingReferences"`. The
  `EnrichmentMethod::field` method stays as the method's default/primary field (decision 8.2).

Design rule that keeps reconcile simple: a method's `extract` returns self-contained `Extraction`
units that carry everything `map_back` needs, so reconcile is a stateless per-unit map with no
cross-row grouping. For funders one unit is one funding reference; for affiliations one unit is one
person (creator or contributor) carrying all of their affiliations.

Stages, gated by `stages_to_run(work_dir, from_scratch)` and `WorkDir` markers, with `only_stage`
to run a single stage:

1. Extract: glob `<input>/**/*.jsonl.gz`, process in parallel. For each record call
   `method.extract(record)`; serialize each `Extraction` to `extractions/part_<file>.jsonl` (one
   part per input file, no shared writer); collect `method.inputs(&extraction)` into the dedup set;
   then write `inputs.jsonl`. Write `extract.done`.
2. Query: build a tokio runtime; read `inputs.jsonl`; filter out hashes already in
   `lookups.checkpoint`; chunk by `ror_batch_size`; run with a `Semaphore(ror_concurrency)`;
   per batch call `svc.match_bulk`; write `M::Lookup` rows to `lookups.jsonl` (keyed by hash) and
   failures to `lookups.failed.jsonl`; update the checkpoint. Write `query.done`. (Lift the
   batching/concurrency loop from the affiliations `query/mod.rs`.)
3. Reconcile: load `lookups.jsonl` into `Lookups<M::Lookup>` (a `HashMap<hash, Lookup>`); stream
   `extractions/`; for each `Extraction` call `method.map_back(extraction, &lookups)`; for each
   returned `EnrichmentParts` run `build_enrichment_record`, validate, and write to the sharded
   gzip output (4b.2) or to `enrichments.failed.jsonl`. Write `reconcile.done`. Populate the report
   match block and stage timings.

#### 4a.4 On-disk stage contract files

Concrete shapes (all JSONL unless noted), written under `<output>/.work` except the final output:

- `extractions/part_*.jsonl`: one serialized `M::Extraction` per line. Funders extraction fields:
  `doi, funding_ref_idx, funder_name, funder_name_hash, existing_identifier,
  existing_identifier_type, award_number, award_title, award_uri, original_funding_reference`.
  Affiliations extraction fields: `doi, field (creators|contributors), idx, source_raw,
  affiliations: [ { affiliation, affiliation_hash, affiliation_raw, existing_ror_id } ]`.
- `inputs.jsonl`: `{ hash, value }` per unique input.
- `lookups.jsonl`: serialized `M::Lookup` keyed by hash. Suggested shape `{ value, hash, ror_id,
  confidence }`.
- `lookups.failed.jsonl`: `{ value, hash, error }`.
- `lookups.checkpoint`: processed hashes (one per line).
- `enrichments/part_*.jsonl.gz`: schema-valid enrichment records (the final output).
- `enrichments.failed.jsonl`: `{ record, errors }` for records that failed schema validation.
- Markers: `extract.done`, `query.done`, `reconcile.done`.

#### 4a.5 report.json

New module `crates/core/src/report.rs`. A `Report` struct serialized to `<output>/report.json`,
matching the spec shape and built from counters accumulated across stages:

```
{
  schema_version, method { name, version },
  data_file { vintage, content_hash },
  coverage { records_in_scope, records_enriched, coverage_rate },
  match { unique_inputs, matched, match_rate, confidence_histogram, failure_taxonomy { no_match, timeout, error } },  // only when a query stage ran
  validation { emitted, schema_failures, schema_failure_taxonomy },
  stage_timings_ms { extract, query, reconcile }
}
```

The transform path (reader::run) emits a report with no `match` block; the staged path fills it
from `lookups.jsonl` / `lookups.failed.jsonl`. Counters come from the existing `RunStats` plus the
new stage timings.

#### 4a.6 manifest.json

New module `crates/core/src/manifest.rs`. A `Manifest` struct serialized to
`<output>/manifest.json`:

```
{
  schema_version,
  method { name = env!("CARGO_PKG_NAME"), version = env!("CARGO_PKG_VERSION") },
  data_file { vintage, content_hash },
  provenance_fingerprint,      // hash of the provenance YAML
  ror { version|path } | null, // when a ROR dataset is supplied
  counters, stage_timings_ms, exit_status
}
```

`vintage` is supplied as a CLI argument or derived from the input path (see open items).
`content_hash` starts as a cheap signature (xxh3 over the sorted list of input file names and
sizes) rather than hashing the full corpus; this is enough for audit and for the later diff
trigger, and can be strengthened later. The version is the workspace version until per-method
versions are split (see 5b).

#### 4a.7 Affiliation method (Stage 7)

Crate `crates/datacite-affiliations`. Port the parser from
`../match-datacite-affiliations-to-ror-ids/src/extract/parser.rs`.

- `Affiliations::try_new(LookupConfig)` builds the method. Note: after dropping the diagnostic
  files, affiliations does not need the ROR registry for the enrichment output (the matched ROR id
  comes straight from the match service), so `--ror-file` is effectively unused here (see open
  items).
- `type Extraction` = one person (creator or contributor) carrying `doi`, the source person object
  with `affiliation` removed, the record field, and the list of affiliations with their hashes,
  raw values, and any existing ROR id.
- `type Lookup` = `{ ror_id, confidence }`.
- `field()` returns `"creators"`; per-record `EnrichmentParts.field` is set to `creators` or
  `contributors`.
- `extract` walks `attributes.creators[*]` and `attributes.contributors[*]`, emitting one
  `Extraction` per person that has at least one affiliation.
- `inputs(extraction)` returns the person's affiliation strings.
- `lookup` is the default; the staged runner calls `match_bulk` with task `affiliation`.
- `map_back` rebuilds the person with ROR-enriched affiliations (matched affiliations get
  `affiliationIdentifier` / `affiliationIdentifierScheme: ROR` / `schemeUri`; unmatched preserve
  their raw value), and returns an `EnrichmentParts` with `action: updateChild` only when the
  person has at least one new match (the `has_new_ror_match` gate from the prototype), so the
  enrichment record set matches the prototype.
- Tests: port the affiliation-specific extract and reconcile tests (reconcile logic exercised
  through `map_back`); query tests via the core `MatchService` (wiremock for the real client); add a
  `tests/end_to_end.rs` running the full staged pipeline against the core mock plus a small ROR
  fixture.

#### 4a.8 Funder method (Stage 8)

Crate `crates/datacite-funders`. Port `identifiers.rs` and the funding-reference parser from
`../match-datacite-funders-to-ror-ids`.

- `identifiers.rs`: `normalize_ror`, `normalize_fundref`, `sniff_identifier` (value-first
  identifier detection), ported verbatim with their tests.
- `Funders::try_new(LookupConfig)` builds the method and lazily loads the Crossref Funder ID to ROR
  crosswalk from `--ror-file` (only the crosswalk map is needed now; ROR display names were only
  used by the dropped diagnostic files). Lazy loading (for example `OnceCell`) avoids paying the
  load cost on extract-only or query-only runs.
- `type Extraction` = one funding reference carrying `doi`, index, funder name and hash, any
  existing identifier and its type (normalised via `identifiers.rs`), award fields, and the
  original funding reference value.
- `type Lookup` = `{ ror_id, confidence }`.
- `field()` returns `"fundingReferences"`.
- `extract` walks `attributes.fundingReferences[*]`, emitting one `Extraction` per reference with a
  non-empty funder name.
- `inputs(extraction)` returns the funder name.
- `map_back` reproduces the prototype routing so the enrichment output matches: skip references
  that already assert a ROR, and skip references whose Crossref Funder ID maps via the crosswalk;
  otherwise, if the match service returned a match for the funder name, emit an `updateChild`
  enrichment that adds `funderIdentifier` / `funderIdentifierType: ROR` / `schemeUri` to the
  original funding reference.
- Tests: port `identifiers_tests`, `extract_tests`, the reconcile logic tests (through `map_back`),
  and query tests via the core mock; add a `tests/end_to_end.rs`.

### 4b. Differs from the spec

- Bulk match endpoint for both methods.
- Compressed, split output instead of one uncompressed file.
- Run identity and run directory supplied by Airflow.
- Manifest emitted by the tool, not an orchestrator.

#### 4b.1 Bulk match endpoint

The spec describes single-search `GET /match?task=&input=`. We use bulk `POST /match/bulk?task=`
with body `{ "inputs": [...] }` for both methods. This is the contract the affiliations prototype
already uses; funders moves from its single GET to the same client (4a.2). `--ror-batch-size` is
the bulk size and applies to both methods.

#### 4b.2 Compressed, split output

The writer (`crates/core/src/writer.rs`) changes from a single uncompressed file to a directory of
gzip parts under `<output>/enrichments/`. Each input file maps to one output part
`part_<seq>.jsonl.gz`, written by the worker that processed it (wrap the part's `BufWriter` in a
`flate2` `GzEncoder`), which also removes the current single-writer-thread bottleneck. Schema
validation stays at the write boundary; failures go to `enrichments.failed.jsonl` (Stage 1). If
part sizes turn out uneven, switch to size-based rolling later. Output is order agnostic.

#### 4b.3 Airflow-owned run directory and vintage

We do not build the spec's framework-owned `runs/<vintage>/<method>@<version>/` path. Airflow
supplies a unique run directory as `--output`; comet-enrich lays out `enrichments/`, the failed
file, `report.json`, `manifest.json`, and `.work/` inside it. The data-file vintage is not in the
Airflow path (that is a trigger timestamp), so it is recorded in `manifest.json` from a CLI
argument or derived from the input path.

#### 4b.4 Manifest emitted by the tool

The spec has the orchestrator emit the run manifest. We emit `manifest.json` from the CLI and core
at run end (4a.6), since orchestration lives in Airflow. This also gives DIFF.md its substrate
(vintage, content hash, provenance fingerprint) without depending on the orchestration layer.

## 5. Not implementing

### 5a. Not at all (handled elsewhere or out of scope)

- The orchestration layer (declarative DAG document plus per-engine adapter). Airflow already owns
  orchestration in `comet-data-infrastructure`.
- Managed dependencies (`ManagedDependency` trait, Marple provisioning, readiness probe, teardown).
  Marple lifecycle is handled by AWS Batch; the CLI just points at `--ror-service-url`.
- The data-file-change trigger. Airflow owns this.
- The persistent record index and the merge step (the prototype SQLite index in the downstream
  `datacite-enrichment` tool).
- Cross-source conflict resolution, and the per-method existing-assignment and disagreement
  outputs. The exclusion logic that affects the enrichment output is still kept (4a.8).

### 5b. Deferred for later

- `--describe` and a data-file-field preflight check.
- Diffing and changefiles (`DIFF.md`). `manifest.json` is designed to be its substrate.
- The polyglot stage-contract path (running a non-Rust method as a subprocess over the same files).
- Per-method crate versions. Crates inherit the workspace version for now, so the manifest reports
  one framework version until we split them.
- Stable output ordering or sharding by `hash(doi)`.

## 6. Implementation order

One stage at a time. Stages 1 to 6 are core work that lands before the first method port. Each
stage is self-contained, testable against the already-working reclassifier where possible, and
should not require reworking earlier stages.

1. **Writer: divert schema failures.** Replace the `anyhow::bail!` on schema violation in
   `crates/core/src/writer.rs` with diversion to `enrichments.failed.jsonl`, attaching the
   validator error per record, and count failures. Validate against the reclassifier end-to-end
   test. Features: 4a.4, 4b.2 (failure-diversion part). Decision: 8.5.

2. **Writer and layout: compressed parts and the run directory.** Change `--output` from a file to
   a directory; write `enrichments/part_NNNN.jsonl.gz`, one part per input file written in
   parallel. Establish the `<output>/.work` scratch location. Update the CLI and the reclassifier
   end-to-end test. Flag the breaking CLI change for the `comet-data-infrastructure` DAG and the
   s5cmd exclude rule. Features: 4b.2, 4b.3.

3. **Core reporting: `report.json` and `manifest.json`.** Produce both in core. Validate on the
   reclassifier (transform path, no match block). Features: 4a.5, 4a.6, 4b.4. Decision: 8.4.

4. **Core: xxh3 dedup store.** Unit-tested in isolation. Features: 4a.1.

5. **Core: match-service client behind a trait.** Fake for runner tests, wiremock for the real
   client. Features: 4a.2, 4b.1.

6. **Core: staged runner and on-disk contract.** Wire `EnrichmentMethod::lookup` into the query
   stage; serialize the contract files; fill the report match block and stage timings. This is the
   largest stage and the one that proves the trait drives a real lookup pipeline. Features: 4a.3,
   4a.4, and the match block of 4a.5. Decisions: 8.1, 8.2.

7. **Port affiliations.** First lookup method; if the port forces non-trivial framework changes,
   fix the framework before Stage 8. Features: 4a.7. Decisions: 8.2, 8.3.

8. **Port funders.** The cheap twin. Features: 4a.8.

## 7. Verification

- Build, test, and lint with the make targets: `make build`, `make test`, `make lint`.
- Each core stage is unit-tested; Stages 1 to 3 are validated end-to-end against the reclassifier.
- Stages 5 and 6 use the core `MatchService` fake for runner tests and wiremock for client tests.
- Stages 7 and 8 each add an end-to-end test that runs the full staged pipeline against the core
  mock match service plus a small ROR registry fixture, asserting the enrichment record set and the
  work-area artifacts.
- Behavioural check per method: for the same input and pinned match service and ROR data, the set
  of records in `enrichments` matches the prototype, allowing for deliberate field name or order
  changes and for record order differing across parts.

## 8. Open decisions

Each decision below needs a choice before the stage that depends on it. The recommended option is
listed first.

### 8.1 How the staged runner gets unique inputs from extractions (gates Stage 6)

The staged runner must collect the set of unique lookup inputs (funder names, affiliation strings)
from the corpus during extract, send them to the match service, then in reconcile join results back
onto each extraction. Today `EnrichmentMethod` exposes `extract -> Extracted<Extraction>` (opaque
to the runner) and `lookup(&[String])`, but nothing lets the generic runner read the input strings
out of an extraction. We need a seam.

- Option A (recommended): add a defaulted method
  `fn inputs(&self, extraction: &Self::Extraction) -> Vec<String> { Vec::new() }` to
  `EnrichmentMethod`. The runner calls it during extract to build the dedup set; `map_back`
  re-derives the same hash to index `Lookups`. Backward compatible (transform methods keep the
  default), single trait preserved, type safe. Touches `method.rs` (one defaulted method). Minor
  duplication: the method names the input in `inputs` and hashes it again inside `map_back`.
- Option B: change `extract` to return the inputs alongside the extraction, for example each item
  becomes `{ inputs: Vec<String>, extraction: Extraction }`. No new method, but it changes the
  `extract` / `Extracted` shape that the transform method also uses, so resource-type-general would
  return empty inputs on every item. More invasive and noisier for transforms than A.
- Option C: reinstate the base plus `LookupMethod` split (the earlier TODO idea). Base
  `EnrichmentMethod` keeps `extract` / `map_back`; `LookupMethod: EnrichmentMethod` adds `inputs`
  and `lookup`. Cleanest separation and the transform signature never mentions lookups, but it is
  the split previously set aside, and it is more churn (runner and CLI dispatch become generic over
  two traits).
- Option D: no trait change; derive inputs from the serialized `extractions.jsonl` by convention
  (each method writes a known column such as a `value` plus `*_hash`, and the runner reads inputs
  from the JSON it just wrote). Honours the spec's "on-disk shape is the contract," but it is
  stringly typed and fragile, and the runner re-parses rows it serialized.

Recommendation: Option A. Smallest change that keeps the single trait, stays type safe, and leaves
transform methods untouched.

### 8.2 Per-record output field for multi-field methods (gates Stage 7)

`build_enrichment_record` stamps a single `field` from `EnrichmentMethod::field()`. Funders and
reclassify each have one field (`fundingReferences`, `types`), but affiliations emits records for
two fields, `creators` and `contributors`, decided per record. So the field must be settable per
enrichment record.

- Option A (recommended): add `field: &'static str` to `EnrichmentParts`; `build_enrichment_record`
  uses it; each method sets it. `EnrichmentMethod::field()` stays as the method's primary field for
  reporting and manifest. Touches `EnrichmentParts`, `build_enrichment_record`, the runner call
  site, and one line in resource-type-general.
- Option B: add an optional override `field: Option<&'static str>` to `EnrichmentParts`, defaulting
  to `method.field()` when `None`. resource-type-general and funders leave it `None`; only
  affiliations sets it. Less churn in existing methods, slightly more logic in the builder.
- Option C: leave the shared types alone and run affiliations as two passes (one for creators, one
  for contributors). Avoids touching shared types but doubles the affiliation wiring and re-reads
  the corpus. Not recommended.

Recommendation: Option A for clarity, or Option B if you want resource-type-general untouched.

### 8.3 `--ror-file` requirement for affiliations (gates Stage 7)

After dropping the diagnostic files, affiliations' enrichment output uses only the match-service
result, so it no longer needs the ROR registry. Funders still needs `--ror-file` for the Crossref
Funder ID to ROR crosswalk exclusion.

- Option A (recommended): make `--ror-file` optional in the shared `LookupArgs`, and have funders
  validate its presence in `try_new`. Avoids a misleading required-but-unused flag for affiliations.
- Option B: keep `--ror-file` required for both (already wired); affiliations ignores it. Less
  churn, but the flag is misleading for affiliations.

Recommendation: Option A.

### 8.4 Data-file vintage source for `manifest.json` (gates Stage 3)

`manifest.json` records the data-file vintage; the Airflow run path encodes a trigger timestamp,
not the vintage.

- Option A (recommended): add an explicit `--vintage` argument that Airflow passes. Unambiguous,
  since Airflow knows the vintage it triggered on.
- Option B: derive it from the input path (the DataCite input directory usually encodes it, for
  example `.../DataCite_Public_Data_File_2024/` or `updated_2024-01`). No new flag, but brittle to
  path naming.
- Option C: both, with `--vintage` overriding the derived value.

Recommendation: Option A.

### 8.5 `enrichments.failed.jsonl` location (gates Stage 1)

The file holds schema-validation failures. It is small and explains coverage drops.

- Option A (recommended): write it in the uploaded output dir, so failures are visible downstream.
- Option B: write it in `.work` (local only), keeping the upload to clean output.

Recommendation: Option A.
</content>
