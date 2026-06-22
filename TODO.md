# TODO

The CLI scaffold is in place; method logic is stubbed. Remaining work:

## Methods
- [ ] resource-type: port the fuzzy reclassifier (rules YAML, matcher, scope) onto `EnrichmentMethod`.
- [ ] affiliations / funders: implement the `extract → query → reconcile` stages, the async
      match-service client, and the staged runner that drives them.

## Core
- [ ] Decide how a stage persists its work between invocations (extractions, resolved matches) —
      i.e. the serialization contract behind `staged.rs`. Needed before the first lookup method.
- [ ] Put the match-service client behind a trait so the query stage can be tested with a mock
      HTTP server instead of a real Marple.
- [ ] When the first lookup method is wired, revisit the `EnrichmentMethod` seam: it currently
      folds the (unbuilt) lookup path into one trait, so `map_back` always gets an empty `Lookups`
      map on the transform path. Consider splitting into a base trait (`extract` / `map_back`, no
      lookups) plus a `LookupMethod` extension so the transform signature never mentions lookups.
- [ ] Perf (only if profiling flags it): `build_enrichment_record` deep-clones the provenance
      template (`contributors` / `resources`) per emitted record — same as the original tool, and
      fine for small provenance blocks. If it ever dominates, send only the dynamic fields over the
      writer channel and serialize from a shared template, or pre-serialize the provenance fragment
      once.

## Downstream (separate repos / configs)
- [ ] comet-data-infrastructure: point `enrich.py` and the DAGs at `comet-enrich <method>`
      (one call) instead of the per-method binaries.
- [ ] Migrate the affiliations/funders enrichment configs to the `enrichment: {sources, resources}`
      shape.
- [ ] If the query stage ever exceeds the 4h Batch attempt timeout, handle it in infra (raise the
      timeout or persist the work dir), not with in-process resume.
