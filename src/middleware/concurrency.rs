//! Controle adaptativo de concorrência (Load Shedding AIMD / TCP Vegas).
use parking_lot::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug)]
struct WindowState {
    limit: usize,
    last_updated: Instant,
}

#[derive(Debug, Clone)]
pub struct AdaptiveConcurrency {
    in_flight: Arc<AtomicUsize>,
    window: Arc<Mutex<WindowState>>,
    target_latency_us: u64,
    min_limit: usize,
    max_limit: usize,
}

impl AdaptiveConcurrency {
    pub fn new(target_latency_ms: u64, min_limit: usize, max_limit: usize) -> Self {
        let min_l = min_limit.max(1);
        let max_l = max_limit.max(min_l);
        Self {
            in_flight: Arc::new(AtomicUsize::new(0)),
            window: Arc::new(Mutex::new(WindowState {
                limit: max_l,
                last_updated: Instant::now(),
            })),
            target_latency_us: target_latency_ms.max(1) * 1_000,
            min_limit: min_l,
            max_limit: max_l,
        }
    }

    pub fn try_acquire(&self) -> bool {
        let limit = self.window.lock().limit;
        let current = self.in_flight.fetch_add(1, Ordering::SeqCst);
        if current >= limit {
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            false
        } else {
            true
        }
    }

    pub fn release(&self, sample_latency_us: u64) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        let mut win = self.window.lock();
        if win.last_updated.elapsed().as_millis() >= 200 {
            if sample_latency_us <= self.target_latency_us {
                win.limit = (win.limit + 2).min(self.max_limit);
            } else {
                let ratio = (self.target_latency_us as f64 / sample_latency_us as f64).clamp(0.5, 0.95);
                let next = ((win.limit as f64) * ratio) as usize;
                win.limit = next.clamp(self.min_limit, self.max_limit);
            }
            win.last_updated = Instant::now();
        }
    }

    pub fn current_limit(&self) -> usize {
        self.window.lock().limit
    }
}
