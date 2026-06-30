# Testing conventions

The tests in this workspace were ported from several earlier prototypes. These are
the conventions we standardise on so equivalent things are written the same way.
Shared helpers live in the dev-only [`comet-test-support`](../crates/test-support)
crate; add it under `[dev-dependencies]` (`comet-test-support = { workspace = true }`)
and reach for it before hand-rolling fixtures.

## Layout

- Unit tests live in an inline `#[cfg(test)] mod tests` opening with `use super::*;`.
- Inside a module, put helpers, fixtures, and fakes **first**, then the `#[test]`
  functions. Don't wedge helpers between tests.
- Integration tests live in `tests/`; share cross-crate helpers via
  `comet-test-support` rather than copying them between files.

## Naming

- `<unit>_<behavior>` in snake_case, no `test_` / `should_` / `it_` prefix.
- Lead with the unit under test so siblings sort together: `match_bulk_success`,
  `extract_emits_on_match`, `fuzzy_match_redundant_is_excluded`.

## Assertions

- Prefer `assert_eq!(actual, expected)` (actual first).
- Compare floats with `comet_test_support::assert_close(actual, expected)` (tolerance
  `1e-9`). Don't hand-roll `f64::EPSILON` / `1e-9` checks for computed values such as
  coverage rates.
- Check enum variants with `assert!(matches!(x, V(..)))`; use
  `let V(..) = x else { panic!() }` only when you need to bind inner values for
  further assertions.

## Errors

- Assert a specific failure with
  `comet_test_support::assert_err_contains(result, "substring")` — one idiom, one
  message format.
- Use a bare `assert!(result.is_err())` only when the error's content is genuinely
  irrelevant to the test.
- Default to bare `.unwrap()` in tests; reserve `.expect("…")` for lines where the
  panic would otherwise be ambiguous. Don't mix the two for identical calls.

## Fixtures and test data

- Build records with `serde_json::json!`; reserve raw `&str` for inputs that must be
  blank or deliberately malformed.
- Lay out gzip input with `gz_input_fixture(&records)` (JSON values) or
  `write_gz_lines(path, &lines)` (raw lines). Read output back with
  `read_enrichment_parts(&output)` or `read_gz_string(&path)`.
- Build `RunOptions` the same way everywhere — single-threaded, `batch_size` 100 (it
  affects neither record nor part counts); a test that specifically exercises batching
  sets `batch_size` explicitly. (`core`'s own tests use a local `run_opts` helper;
  it can't live in `comet-test-support`, which is kept free of `core` types.)
- Reference committed config with `config_path("provenance/…")` instead of
  re-inlining YAML or building `concat!(env!("CARGO_MANIFEST_DIR"), …)` paths. Inline
  YAML stays only in tests that exercise provenance/rules parsing itself.

## Mocks

- Use the in-memory `FakeMatchService` (in `core`, `crate::match_service`) for unit
  and staged tests; `FakeMatchService::erroring()` simulates a sustained outage. It
  stays in `core` rather than `comet-test-support` because it implements `core`'s
  `MatchService` trait.
- Use `wiremock` only where the real HTTP client (`MarpleClient`) is the thing under
  test, i.e. `crates/core/tests/match_service.rs`.

## Async

- Bare `#[tokio::test]` unless a test genuinely needs `flavor = "multi_thread"`.
