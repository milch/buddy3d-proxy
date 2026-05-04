//! Capped exponential backoff with jitter, suitable for reconnect loops.
//!
//! Schedule: 1s, 2s, 4s, 8s, 16s, then 30s thereafter. Each delay is
//! multiplied by a jitter factor uniformly sampled from `[0.8, 1.2]` so
//! synchronized clients don't retry in lockstep. `reset()` returns the
//! sequence to its first delay; call after a confirmed success.

use rand::RngExt;
use std::time::Duration;

#[derive(Debug)]
pub struct ExpBackoff {
    attempt: u32,
    base_secs: [u64; 5],
    cap_secs: u64,
}

impl Default for ExpBackoff {
    fn default() -> Self {
        Self {
            attempt: 0,
            base_secs: [1, 2, 4, 8, 16],
            cap_secs: 30,
        }
    }
}

impl ExpBackoff {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset(&mut self) {
        self.attempt = 0;
    }

    /// Advance the schedule and return the next delay (with jitter applied).
    pub fn next_delay(&mut self) -> Duration {
        let raw = self
            .base_secs
            .get(self.attempt as usize)
            .copied()
            .unwrap_or(self.cap_secs);
        self.attempt = self.attempt.saturating_add(1);
        let jitter: f64 = rand::rng().random_range(0.8..=1.2);
        Duration::from_millis((raw as f64 * 1000.0 * jitter) as u64)
    }

    pub fn attempt(&self) -> u32 {
        self.attempt
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_delay_is_around_one_second() {
        let mut b = ExpBackoff::new();
        let d = b.next_delay();
        assert!(d >= Duration::from_millis(800));
        assert!(d <= Duration::from_millis(1200));
    }

    #[test]
    fn second_delay_is_around_two_seconds() {
        let mut b = ExpBackoff::new();
        b.next_delay();
        let d = b.next_delay();
        assert!(d >= Duration::from_millis(1600));
        assert!(d <= Duration::from_millis(2400));
    }

    #[test]
    fn caps_at_thirty_seconds() {
        let mut b = ExpBackoff::new();
        for _ in 0..10 {
            b.next_delay();
        }
        let d = b.next_delay();
        assert!(d >= Duration::from_millis(24_000));
        assert!(d <= Duration::from_millis(36_000));
    }

    #[test]
    fn reset_returns_to_first_delay() {
        let mut b = ExpBackoff::new();
        for _ in 0..5 {
            b.next_delay();
        }
        b.reset();
        let d = b.next_delay();
        assert!(d >= Duration::from_millis(800));
        assert!(d <= Duration::from_millis(1200));
    }
}
