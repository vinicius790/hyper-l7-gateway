//! Token bucket thread-safe com auto-limpeza de chaves inativas e metadados de quota.
use parking_lot::Mutex;
use std::collections::HashMap;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy)]
pub struct RateLimitDecision {
    pub allowed: bool,
    pub limit: u64,
    pub remaining: u64,
    pub retry_after_secs: u64,
}

#[derive(Debug)]
pub struct TokenBucket {
    capacity: f64,
    tokens: f64,
    refill_per_sec: f64,
    last: Instant,
}

impl TokenBucket {
    pub fn new(capacity: u64, refill_per_sec: f64) -> Self {
        let cap = capacity as f64;
        Self {
            capacity: cap,
            tokens: cap,
            refill_per_sec,
            last: Instant::now(),
        }
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.capacity);
        self.last = now;
    }

    pub fn try_acquire(&mut self, n: f64) -> bool {
        self.refill();
        if self.tokens >= n {
            self.tokens -= n;
            true
        } else {
            false
        }
    }

    pub fn check_decision(&mut self) -> RateLimitDecision {
        let allowed = self.try_acquire(1.0);
        let remaining = self.tokens.floor().max(0.0) as u64;
        let retry_after_secs = if allowed || self.refill_per_sec <= 0.0 {
            0
        } else {
            (((1.0 - self.tokens).max(0.0) / self.refill_per_sec).ceil() as u64).max(1)
        };
        RateLimitDecision {
            allowed,
            limit: self.capacity as u64,
            remaining,
            retry_after_secs,
        }
    }

    pub fn available(&mut self) -> f64 {
        self.refill();
        self.tokens
    }
}

#[derive(Debug, Default)]
pub struct RateLimiterMap {
    inner: Mutex<HashMap<String, TokenBucket>>,
    capacity: u64,
    refill_per_sec: f64,
}

impl RateLimiterMap {
    pub fn new(capacity: u64, refill_per_sec: f64) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            capacity,
            refill_per_sec,
        }
    }

    pub fn allow(&self, key: &str) -> bool {
        self.check(key).allowed
    }

    pub fn check(&self, key: &str) -> RateLimitDecision {
        let mut map = self.inner.lock();
        // Evicção preventiva de chaves antigas caso o mapa passe de 10.000 entradas
        if map.len() > 10_000 {
            let cutoff = Duration::from_secs(300);
            map.retain(|_, b| b.last.elapsed() < cutoff);
        }
        let bucket = map
            .entry(key.to_string())
            .or_insert_with(|| TokenBucket::new(self.capacity, self.refill_per_sec));
        bucket.check_decision()
    }

    pub fn available(&self, key: &str) -> f64 {
        let mut map = self.inner.lock();
        let bucket = map
            .entry(key.to_string())
            .or_insert_with(|| TokenBucket::new(self.capacity, self.refill_per_sec));
        bucket.available()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn burst_then_block() {
        let mut b = TokenBucket::new(3, 1000.0);
        assert!(b.try_acquire(1.0));
        assert!(b.try_acquire(1.0));
        assert!(b.try_acquire(1.0));
        assert!(!b.try_acquire(1.0));
    }

    #[test]
    fn refill_over_time() {
        let mut b = TokenBucket::new(1, 50.0);
        assert!(b.try_acquire(1.0));
        assert!(!b.try_acquire(1.0));
        thread::sleep(Duration::from_millis(30));
        assert!(b.try_acquire(1.0));
    }

    #[test]
    fn map_per_key() {
        let m = RateLimiterMap::new(1, 0.001);
        assert!(m.allow("a"));
        assert!(!m.allow("a"));
        assert!(m.allow("b"));
    }
}
