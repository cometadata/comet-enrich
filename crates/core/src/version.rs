//! Minimum artifact version that current tooling can consume.

/// First release whose run artifacts carry content keys and DOI deduplication.
/// Releases and stage artifacts older than this cannot be diffed or reused.
pub const MIN_ARTIFACT_VERSION: &str = "0.4.0";

fn parse(version: &str) -> Option<(u64, u64, u64)> {
    let mut parts = version.trim().split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts.next()?.parse().ok()?;
    parts.next().is_none().then_some((major, minor, patch))
}

/// Whether `version` is a `major.minor.patch` string not below `min`.
/// Anything that does not parse is treated as below the minimum.
#[must_use]
pub fn is_at_least(version: &str, min: &str) -> bool {
    match (parse(version), parse(min)) {
        (Some(v), Some(m)) => v >= m,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_at_least_orders_numerically_and_rejects_garbage() {
        assert!(is_at_least("0.4.0", MIN_ARTIFACT_VERSION));
        assert!(is_at_least("0.10.0", MIN_ARTIFACT_VERSION));
        assert!(!is_at_least("0.3.9", MIN_ARTIFACT_VERSION));
        assert!(!is_at_least("", MIN_ARTIFACT_VERSION));
        assert!(!is_at_least("0.4", MIN_ARTIFACT_VERSION));
    }
}
