//! Timeout de request em ms.
pub fn timed_out(started_ms: u64, now_ms: u64, limit_ms: u64) -> bool {
    now_ms.saturating_sub(started_ms) >= limit_ms
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fires() {
        assert!(timed_out(0, 200, 100));
    }
}
