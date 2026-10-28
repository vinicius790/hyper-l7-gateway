//! Circuit breaker resiliente: Closed -> Open -> HalfOpen -> Closed.
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

#[derive(Debug)]
struct Inner {
    state: CircuitState,
    failures: u32,
    successes_half: u32,
    opened_at: Option<Instant>,
    failure_threshold: u32,
    open_for: Duration,
    half_open_success: u32,
}

#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    inner: Arc<Mutex<Inner>>,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, open_ms: u64, half_open_success: u32) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                state: CircuitState::Closed,
                failures: 0,
                successes_half: 0,
                opened_at: None,
                failure_threshold: failure_threshold.max(1),
                open_for: Duration::from_millis(open_ms),
                half_open_success: half_open_success.max(1),
            })),
        }
    }

    fn transition(&self, now: Instant) {
        let mut g = self.inner.lock();
        if g.state == CircuitState::Open {
            if let Some(t) = g.opened_at {
                if now.duration_since(t) >= g.open_for {
                    g.state = CircuitState::HalfOpen;
                    g.successes_half = 0;
                }
            }
        }
    }

    pub fn allow(&self) -> bool {
        self.transition(Instant::now());
        let g = self.inner.lock();
        match g.state {
            CircuitState::Closed | CircuitState::HalfOpen => true,
            CircuitState::Open => false,
        }
    }

    pub fn state(&self) -> CircuitState {
        self.transition(Instant::now());
        self.inner.lock().state
    }

    pub fn on_success(&self) {
        let mut g = self.inner.lock();
        match g.state {
            CircuitState::HalfOpen => {
                g.successes_half += 1;
                if g.successes_half >= g.half_open_success {
                    g.state = CircuitState::Closed;
                    g.failures = 0;
                    g.opened_at = None;
                }
            }
            CircuitState::Closed => {
                g.failures = 0;
            }
            CircuitState::Open => {}
        }
    }

    pub fn on_failure(&self) {
        let mut g = self.inner.lock();
        match g.state {
            CircuitState::HalfOpen => {
                g.state = CircuitState::Open;
                g.opened_at = Some(Instant::now());
                g.failures = 0;
                g.successes_half = 0;
            }
            CircuitState::Closed => {
                g.failures += 1;
                if g.failures >= g.failure_threshold {
                    g.state = CircuitState::Open;
                    g.opened_at = Some(Instant::now());
                }
            }
            CircuitState::Open => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn opens_after_threshold() {
        let cb = CircuitBreaker::new(3, 10_000, 1);
        assert!(cb.allow());
        cb.on_failure();
        cb.on_failure();
        assert_eq!(cb.state(), CircuitState::Closed);
        cb.on_failure();
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(!cb.allow());
    }

    #[test]
    fn half_open_then_close() {
        let cb = CircuitBreaker::new(1, 20, 1);
        cb.on_failure();
        assert_eq!(cb.state(), CircuitState::Open);
        thread::sleep(Duration::from_millis(30));
        assert!(cb.allow());
        assert_eq!(cb.state(), CircuitState::HalfOpen);
        cb.on_success();
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn half_open_failure_reopens() {
        let cb = CircuitBreaker::new(1, 20, 1);
        cb.on_failure();
        thread::sleep(Duration::from_millis(30));
        assert!(cb.allow());
        cb.on_failure();
        assert_eq!(cb.state(), CircuitState::Open);
    }
}
