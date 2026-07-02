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

- Parallel reader for `*.jsonl.gz` data-file inputs (rayon fan-out, glob), now split across
  `crates/core/src/fanout.rs` (shared scanning helpers) and `crates/core/src/transform.rs` (the
  transform runner).
- The `EnrichmentMethod` trait with `extract` / `map_back` (`crates/core/src/method.rs`). We keep
  the single-trait shape; we are not splitting it into a separate `LookupMethod` extension. Two
  small, backward-compatible additions are described in 4a.3. (An early default `lookup` method was
  removed in the cleanup pass — the runners resolve lookups through `MatchService` directly.)
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
- Stage-planning scaffolding (`Stage`, `WorkDir`, `stages_to_run`, `LookupConfig`), driven by the
  staged runner in Stage 6 and now living alongside it in `crates/core/src/staged_run.rs`.

## 2. Comparisons

### 2a. Spec on-disk contract vs prototype files

| Spec contract file | Prototype equivalent (funders / affiliations) | Decision |
|---|---|---|
| `extractions.jsonl` | `doi_funders.jsonl` / `doi_author_affiliations.jsonl` | Rename to `extractions.jsonl`, one serialized extraction per line. |
| dedup input list | `unique_funder_names.json` / `unique_affiliations.json` | Standardise to `inputs.jsonl`, one row per unique input keyed by hash, in the work area. |
| `lookups.jsonl` | `ror_matches.jsonl` | Rename. |
| `lookups.failed.jsonl` | `ror_matches.failed.jsonl` | Rename. |
| `lookups.checkpoint` | `ror_matches.checkpoint` | Dropped: no within-stage resume (see 4a.3); `.done` markers are the resume unit. |
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
    extract.done query.done reconcile.done   stage markers
```

Notes:

- `--output` changes from a file to a directory. This is a breaking CLI change to coordinate with
  the `comet-data-infrastructure` DAG that invokes `comet-enrich <method>`.
- The work area is always `<output>/.work`; there is no separate work-dir option.
- The reclassifier (transform, no lookup) produces only `enrichments/`,
  `enrichments.failed.jsonl`, `report.json`, and `manifest.json`. It has no `.work` lookup files.
- Output is split into compressed parts that roll by output volume, not by input-file count. Record
  order across parts is not stable run to run, which is acceptable. Diffing (a later track) sorts a
  manifest, not the data.

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

- `hash_input(s: &str, bits: HashBits) -> String`, where `HashBits` is `Xxh3_64` (default, 16-hex,
  `format!("{:016x}", xxh3_64(..))`) or `Xxh3_128` (32-hex, `xxh3_128`). The 64-bit default is the
  same hash the prototypes use (`hash_funder_name` / `hash_affiliation`), so persisted hashes match
  and a golden parity test pins it. Hashing produces a raw `u128` (the 64-bit case zero-extended)
  that doubles as the dedup/collision key and is formatted to hex through one code path. Note:
  `DIFF.md` calls for xxh3-128 for the future *diff content-hash*; that is a separate concern from
  this configurable dedup hash.
- The dedup itself is a `BTreeSet<String>` of input *values* (ordered for deterministic
  `inputs.jsonl`), accumulated during extract. Dedup is always by value, never by hash. The helper
  writes `inputs.jsonl` rows of `{ "hash": <hash>, "value": <input> }` and, in the same pass,
  detects collisions loudly: if two distinct values produce the same hash it errors instead of
  emitting duplicate hash keys (which `Lookups`, keyed by hash, would otherwise silently overwrite —
  applying one match to the wrong input). The collision `seen` set is O(unique inputs) and
  effectively unused at 128-bit; this is noted in code as a possible memory concern for very large
  unique sets.
- Hash width is fixed for a whole run. The `--hash-bits {64,128}` CLI flag, recording
  `hash { algorithm, bits }` in `manifest.json` (lookup methods only, like the `match` block), and
  pinning the width in the run dir + refusing a mismatched resume, are wired in **Stage 6**, where
  the dedup store is actually consumed. Stage 4 builds the core capability (the `HashBits` enum,
  configurable `hash_input`, collision detection) standalone and unit-tested.

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
- ~~`Checkpoint`~~ — a within-stage resume ledger (`lookups.checkpoint`) was built in Stage 5 and
  then **deleted** by the post-Stage-6 soundness pass (see 4a.3): the query stage re-runs whole,
  and the `.done` markers are the only resume unit.

Stage-5 implementation notes (as built):
- The trait/client deliberately differ from the spec above: the trait is `Send + Sync` (the runner
  shares it across `tokio::spawn`), and `MatchService`/`MarpleClient` live in flat modules
  `match_service.rs` + `checkpoint.rs` (no `match_service/` dir). The URL is built with
  `reqwest::Url` (`path_segments_mut().pop_if_empty()` + `.query(...)`), so `urlencoding` is not a
  dependency. The client retries `429`, `408`, and `5xx` with capped exponential backoff
  (`MAX_RETRIES = 4`), honouring `Retry-After`; every wait is clamped to a 2-minute
  `MAX_RETRY_WAIT`. `413` and other `4xx` fail fast.
- **TODO (verify against the live Marple service):** `match_bulk` assumes `/match/bulk` returns
  results in **input order** and validates only the result count, not an echo of each input. Confirm
  the bulk endpoint's ordering guarantee before relying on it in production; if it does not hold, add
  an input echo/correlation.

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

Stage-6 implementation notes (as built):

- `run_staged` lives in `crates/core/src/staged_run.rs`, takes `svc: &Arc<dyn MatchService>` (not
  `&dyn`, because the query stage shares it across `tokio::spawn`), and returns a `Report`. The CLI
  wraps that with `Manifest::from_report(meta, exit_status, report, HashInfo)` (new constructor) and
  a new optional `Manifest.hash` (`HashInfo { algorithm, bits }`), so the transform path's
  `Manifest::build` and the reclassifier test are untouched.
- A generic runner can't name a method's lookup fields, so it builds `M::Lookup` from a service
  result through a new `From<MatchHit>` seam (`MatchHit { id, confidence }` in core). Both ROR
  methods share a core `RorLookup { ror_id, confidence }` as their `type Lookup`. Runner bound:
  `M::Lookup: Serialize + DeserializeOwned + From<MatchHit> + Send + Sync + 'static`.
- The dedup-hash width moved into `LookupConfig.hash_bits` (rather than a standalone `run_staged`
  argument), because a method's `extract` hashes each occurrence and the runner hashes `inputs.jsonl`
  — both must agree. The width is pinned to `.work/hash.bits` during extract and a mismatched resume
  is refused; it is recorded as `hash { algorithm, bits }` in `manifest.json` for lookup methods.
- The query stage spawns one task per batch (concrete results only) bounded by a
  `Semaphore(ror_concurrency)`; `lookups.jsonl`/`lookups.failed.jsonl` open in append mode on resume.
  The checkpoint is saved once at the end of query (the cadence decision from 4a.2): a crash only
  costs this run's query work, which a rerun regenerates.
- `only_stage` runs exactly that stage (its predecessor `.done` markers must exist) rather than
  intersecting with `stages_to_run`, so an explicit `query`/`reconcile` re-run always executes.
- Extract persists a small `extract.stats.json` sidecar (records scanned, lines malformed, file
  counts) so coverage in the report is correct on a resume that skips extract. The report match
  block is recomputed from `inputs.jsonl` / `lookups.jsonl` / `lookups.failed.jsonl` at the end, so
  it is correct regardless of which stages ran this invocation.
- `deny.toml` gained the TLS/hash-stack permissive licenses (ISC, BSD-3-Clause, BSL-1.0,
  CDLA-Permissive-2.0) that the reqwest/rustls + `xxhash-rust` graph requires; this allow-list gap
  predated Stage 6 but the lint gate first needed it green here.

Post-Stage-6 cleanup (a `/simplify` pass): the `EnrichmentMethod::field()` trait method was
**removed** — decision 8.2's per-record `EnrichmentParts.field` fully supersedes it, and after Stage
6 `field()` had zero call sites (the manifest keys off the method name, not `field()`), so it was a
required-but-unused seam every method had to satisfy. The parallel fan-out scaffolding shared by the
transform and staged runners (the per-file `FileError`, input globbing, and rayon pool construction)
was lifted into `crates/core/src/fanout.rs` instead of being duplicated in `reader.rs` and
`staged_run.rs`, and the coverage-rate/validation construction was factored into `Coverage::new` /
`Validation::new` used by both report-building paths.

Post-Stage-6 soundness pass (an adversarial review + audit found the staged report could certify a
lossy run as `success`). Supersedes the within-stage-resume parts of 4a.2 above:

- **Within-stage resume removed.** `checkpoint.rs` is **deleted** and the query stage now truncates
  and re-runs whole (no checkpoint, no append). Production tears `.work` down on a Batch retry (it is
  excluded from the S3 upload), so within-stage resume could never fire there; locally, **cross-stage**
  resume (`stages_to_run` + `.done` markers) is kept — a crashed stage re-runs whole, the completed
  ones are skipped. This removes the duplicate-rows-on-resume bug and the vestigial checkpoint by
  construction. (Supersedes the 4a.2 `Checkpoint` bullet and the §5b "checkpoint writes atomically"
  note — there is no checkpoint.)
- **Report is a faithful projection of persisted per-stage stats.** `extract.stats.json` gains
  `in_scope_units` + `skipped`; a new `reconcile.stats.json` holds `emitted` + `schema_failures`;
  `build_report` reads both, so a rerun that skips a stage (incl. a no-op rerun of a *complete* run)
  reports the truth instead of `emitted: 0`. The staged path no longer hardcodes `schema_failures: 0`
  or drops skip reasons.
- **Honest, shared `exit_status`** (`manifest::exit_status`): `partial` on any of `files_failed`,
  `schema_failures`, unresolved lookups (`failure_taxonomy.lost()` — batch errors *and* timeouts),
  or an incomplete pipeline; used by both run paths. A whole-batch lookup error is non-fatal
  (recorded on each failed row with a structured `kind` → `partial`), not an abort. The
  timeout/error split within the taxonomy is informational only and cannot affect the verdict.
- **Coverage is extraction-unit** (decision D3): `records_in_scope` = extraction units the method
  produced, `records_enriched` = `emitted`; since each unit yields ≤1 record, `coverage_rate ∈ [0,1]`.
  This keeps `emitted` identical to the prototypes' "Enriched records" count.

Post-review cleanup pass (July 2026), before the Stage 7/8 ports:

- **Failure rows carry a structured `kind`** (`no_match` / `error`), written by the query stage at
  the moment the failure happens; the report taxonomy no longer classifies by error-message text,
  and `exit_status` counts *all* unresolved lookups (timeouts included) toward `partial`.
- **Guards:** an input directory with no `*.jsonl.gz` files is an error (checked before any
  artifacts are cleared), and `--from-scratch` combined with a single stage is rejected instead of
  silently ignored.
- **Trait slimmed:** `EnrichmentMethod::lookup` was removed (neither runner called it; the ports
  resolve lookups through `MatchService` directly). The trait is `extract` / `inputs` / `map_back`.
- **Modules:** `staged.rs` (stage planning) merged into `staged_run.rs`; `run.rs` renamed to
  `options.rs`. Crates renamed to a single prefix: `comet-enrich-core`,
  `comet-enrich-test-support`.
- **UX:** all three staged stages report progress with the same indicatif bar as the transform
  path; schema validation happens before the writer-lane lock so it parallelizes.
- **Dependencies:** `serde_yaml` (archived upstream) replaced by `serde_yaml_ng`; `jsonschema`
  upgraded from 0.18 to the current release (`Validator` / `validator_for` API).

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
- `lookups.failed.jsonl`: `{ value, hash, kind, error }`, where `kind` is `no_match` (the service
  answered and found nothing) or `error` (the input was never resolved; downgrades the run to
  `partial`).
- `enrichments/part_*.jsonl.gz`: schema-valid enrichment records (the final output).
- `enrichments.failed.jsonl`: `{ record, errors }` for records that failed schema validation.
- Markers: `extract.done`, `query.done`, `reconcile.done`.

#### 4a.5 report.json

As built, there is no separate `report.json`: the `Report` struct lives in
`crates/core/src/manifest.rs` and ships as the `report` block inside `manifest.json` (4a.6). The
shape below is otherwise as planned:

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

The transform path (`transform::run`) emits a report with no `match` block; the staged path fills
it from `lookups.jsonl` / `lookups.failed.jsonl`. Counters come from the existing `RunStats` plus
the new stage timings.

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
  comes straight from the match service), so the affiliations command has no `--ror-file` flag
  (decision 8.3, implemented).
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
gzip parts under `<output>/enrichments/`. Final parts roll by compressed output size
(`--output-part-size-mib`, default 256) and may be written through multiple DOI-hash writer lanes
(`--output-writer-lanes`, default 1). Lane-local temp files are renamed into contiguous
`part_<seq>.jsonl.gz` names after a successful run. Schema validation stays at the write boundary;
failures go to `enrichments.failed.jsonl` (Stage 1). Output is order agnostic.

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
- Atomic file writes. The writers (`writer.rs` enrichment parts and `enrichments.failed.jsonl`, and
  `dedup.rs` `inputs.jsonl`) use `File::create` (truncate-then-stream), so a mid-write failure leaves
  a truncated file. Tolerated today: artifacts are regenerable and reruns overwrite, `.work` scratch
  is read back only behind `.done` stage markers, and the final outputs are not re-read within a run.
  If we want write atomicity it should be one cross-cutting pass (temp file + rename across *all*
  writers), not a one-off on a single file. (Surfaced by the Stage 4 adversarial review of
  `inputs.jsonl`, but it is a pre-existing property of the Stage 1–2 writer.) Exception: the stage
  `.done` markers publish atomically (temp + rename), since they are the resume unit; the remaining
  writers and `fsync` are still deferred here.

## 6. Implementation order

One stage at a time. Stages 1 to 6 are core work that lands before the first method port. Each
stage is self-contained, testable against the already-working reclassifier where possible, and
should not require reworking earlier stages.

1. **Writer: output directory + divert schema failures.** Make `--output` a directory: write the
   main output to `<output>/enrichments.jsonl` and divert schema-validation failures to
   `<output>/enrichments.failed.jsonl` (with the validator error per record) instead of aborting;
   `RunStats` gains `schema_failures`. Remove `--work-dir` (the work area is fixed at
   `<output>/.work`). Validate against the reclassifier end-to-end test. Flag the breaking
   `--output` change for the `comet-data-infrastructure` DAG and the s5cmd exclude rule. Features:
   4a.4, 4b.2 (failure-diversion part), 4b.3 (output-directory part). Decision: 8.5. Done.

2. **Writer: compressed split output.** Split `<output>/enrichments.jsonl` into rolling
   `<output>/enrichments/part_NNNN.jsonl.gz` files, with optional parallel writer lanes. Update the
   reclassifier end-to-end test. Features: 4b.2.

3. **Core reporting: `report.json` and `manifest.json`.** Produce both in core. Validate on the
   reclassifier (transform path, no match block). Features: 4a.5, 4a.6, 4b.4. Decision: 8.4.

4. **Core: xxh3 dedup store.** Unit-tested in isolation. Features: 4a.1.

5. **Core: match-service client behind a trait.** Fake for runner tests, wiremock for the real
   client. Features: 4a.2, 4b.1.

6. **Core: staged runner and on-disk contract.** Wire `EnrichmentMethod::lookup` into the query
   stage; serialize the contract files; fill the report match block and stage timings. This is the
   largest stage and the one that proves the trait drives a real lookup pipeline. Features: 4a.3,
   4a.4, and the match block of 4a.5. Decisions: 8.1, 8.2. Also wires the dedup-hash width: a
   `--hash-bits {64,128}` flag (default 64), pinned in the run dir and validated on resume (a
   mismatched width silently breaks the hash join), and recorded as `hash { algorithm, bits }` in
   `manifest.json` for lookup methods (see 4a.1). Done.

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

### 8.3 `--ror-file` requirement for affiliations (gates Stage 7) — DECIDED

After dropping the diagnostic files, affiliations' enrichment output uses only the match-service
result, so it no longer needs the ROR registry. Funders still needs `--ror-file` for the Crossref
Funder ID to ROR crosswalk exclusion.

**Decided and implemented** (July 2026 cleanup pass): `--ror-file` moved out of the shared
`LookupArgs` and `LookupConfig` entirely. Funders has its own required `--ror-file` flag (clap
enforces it, carried in a funders-local `Config`); the affiliations command has no such flag.

### 8.4 Data-file vintage source for `manifest.json` (gates Stage 3) — RESOLVED

`manifest.json` records the data-file vintage; the Airflow run path encodes a trigger timestamp,
not the vintage.

**Resolved by `--source-release-date`** (a generalisation of Option A): the CLI takes repeatable
`--source-release-date name=YYYY-MM-DD` arguments, recorded in the manifest's `sources` map (e.g.
`datacite`, `ror`), so every consumed source carries its release date rather than a single vintage.

### 8.5 `enrichments.failed.jsonl` location (gates Stage 1)

The file holds schema-validation failures. It is small and explains coverage drops.

- Option A (recommended): write it in the uploaded output dir, so failures are visible downstream.
- Option B: write it in `.work` (local only), keeping the upload to clean output.

Recommendation: Option A.
</content>
