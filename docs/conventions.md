# Conventions

## Tests

Use these conventions when adding or updating tests.

Shared helpers live in the dev-only [`comet-test-support`](../crates/test-support)
crate. Add it under `[dev-dependencies]` and use it before adding local fixtures or
utilities.

### Layout

- Put unit tests in an inline `#[cfg(test)] mod tests` module.
- Start the test module with `use super::*;`.
- Put helpers, fixtures, and fakes before the `#[test]` functions.
- Put integration tests in `tests/`.
- Share cross-crate test helpers through `comet-test-support`.

### Naming

Use `<unit>_<behavior>` in snake_case.

Do not use `test_`, `should_`, or `it_` prefixes.

Lead with the unit under test so related tests sort together.

### Assertions

- Prefer `assert_eq!(actual, expected)`, with the actual value first.
- Use `comet_test_support::assert_close(actual, expected)` for float comparisons.
- Check enum variants with `assert!(matches!(x, V(..)))`.

### Errors

- Assert specific failures with
  `comet_test_support::assert_err_contains(result, "substring")`.
- Use bare `assert!(result.is_err())` only when the error content does not matter.
- Default to bare `.unwrap()` in tests.
- Use `.expect("…")` only where the panic would otherwise be unclear.

### Fixtures and test data

- Build records with `serde_json::json!`.
- Use raw `&str` only for inputs that must be blank or deliberately malformed.
- Use `gz_input_fixture(&records)` for gzip input made from JSON values.
- Use `write_gz_lines(path, &lines)` for gzip input made from raw lines.
- Read output with `read_enrichment_parts(&output)` or `read_gz_string(&path)`.

Default `RunOptions` to single-threaded with `batch_size: 100`. Tests that exercise
batching should set `batch_size` explicitly.

Use `config_path("provenance/…")` for committed config files instead of re-inlining
YAML or building manifest-relative paths.

Inline YAML should only be used in tests that check provenance or rules parsing.

### Mocks

Use the in-memory `FakeMatchService` for unit and staged tests.

Use `FakeMatchService::erroring()` to simulate a sustained outage.

Use `wiremock` only when the real HTTP client is under test.

### Async

Use bare `#[tokio::test]` unless the test genuinely needs `flavor = "multi_thread"`.