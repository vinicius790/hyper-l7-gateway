//! Bounded retry delay for callers that have already checked request idempotence.
pub fn backoff(base: u64, attempt: u32) -> u64 {
    base.saturating_mul(1u64 << attempt.min(8))
}
/// Deterministic full jitter, useful for reproducible retry simulations.
pub fn jittered_backoff(base: u64, attempt: u32, cap: u64, seed: u64) -> u64 {
    let limit = backoff(base, attempt).min(cap);
    let mut x = seed
        .wrapping_add(u64::from(attempt))
        .wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^= x >> 31;
    if limit == u64::MAX {
        x
    } else {
        x % (limit + 1)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded() {
        assert_eq!(backoff(10, 2), 40);
        assert_eq!(backoff(u64::MAX, 8), u64::MAX);
        for n in 0..100 {
            assert!(jittered_backoff(10, 5, 100, n) <= 100);
        }
    }
}

/// Compatibility entry point retaining the original parameter order.
pub fn backoff_ms(attempt: u32, base: u64) -> u64 {
    backoff(base, attempt)
}
