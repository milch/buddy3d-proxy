//! Token bucket that paces WebRTC connect attempts over the long run.
//!
//! Every attempt needs at least one token in the bucket. A failed attempt
//! spends one, a successful attempt earns one back, and while below capacity
//! the bucket refills at one token per `refill_period`. With the defaults
//! (10 tokens, 1 per 5 min) a camera that stays unreachable gets ten retries
//! paced by `ExpBackoff`, then one attempt every five minutes.

use std::time::Duration;
use tokio::time::Instant;

pub const DEFAULT_CAPACITY: u32 = 10;
pub const DEFAULT_REFILL_PERIOD: Duration = Duration::from_secs(5 * 60);

#[derive(Debug)]
pub struct ReconnectBudget {
    tokens: u32,
    capacity: u32,
    refill_period: Duration,
    last_refill: Instant,
}

impl Default for ReconnectBudget {
    fn default() -> Self {
        Self::new(DEFAULT_CAPACITY, DEFAULT_REFILL_PERIOD)
    }
}

impl ReconnectBudget {
    /// Starts full.
    pub fn new(capacity: u32, refill_period: Duration) -> Self {
        Self {
            tokens: capacity,
            capacity,
            refill_period,
            last_refill: Instant::now(),
        }
    }

    /// How long until the next attempt is allowed; zero if one is allowed now.
    pub fn wait_time(&mut self) -> Duration {
        self.refill();
        if self.tokens > 0 {
            Duration::ZERO
        } else {
            self.refill_period.saturating_sub(self.last_refill.elapsed())
        }
    }

    pub fn record_success(&mut self) {
        self.refill();
        self.tokens = (self.tokens + 1).min(self.capacity);
    }

    pub fn record_failure(&mut self) {
        self.refill();
        self.tokens = self.tokens.saturating_sub(1);
    }

    pub fn tokens(&mut self) -> u32 {
        self.refill();
        self.tokens
    }

    pub fn capacity(&self) -> u32 {
        self.capacity
    }

    fn refill(&mut self) {
        // A full bucket doesn't accrue; the refill clock starts on the first
        // token spent.
        if self.tokens >= self.capacity {
            self.last_refill = Instant::now();
            return;
        }
        let earned = self.last_refill.elapsed().as_nanos() / self.refill_period.as_nanos();
        if earned == 0 {
            return;
        }
        let earned = earned.min(self.capacity as u128) as u32;
        self.tokens = (self.tokens + earned).min(self.capacity);
        if self.tokens >= self.capacity {
            self.last_refill = Instant::now();
        } else {
            // Keep the remainder so partial progress toward the next token isn't lost.
            self.last_refill += self.refill_period * earned;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PERIOD: Duration = Duration::from_secs(300);

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn starts_full_and_allows_attempts() {
        let mut b = ReconnectBudget::new(10, PERIOD);
        assert_eq!(b.tokens(), 10);
        assert_eq!(b.wait_time(), Duration::ZERO);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn failures_drain_then_block_until_refill() {
        let mut b = ReconnectBudget::new(3, PERIOD);
        for _ in 0..3 {
            b.record_failure();
        }
        assert_eq!(b.tokens(), 0);
        assert_eq!(b.wait_time(), PERIOD);
        tokio::time::advance(Duration::from_secs(100)).await;
        assert_eq!(b.wait_time(), Duration::from_secs(200));
        tokio::time::advance(Duration::from_secs(200)).await;
        assert_eq!(b.tokens(), 1);
        assert_eq!(b.wait_time(), Duration::ZERO);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn success_earns_a_token_up_to_capacity() {
        let mut b = ReconnectBudget::new(3, PERIOD);
        b.record_failure();
        b.record_failure();
        b.record_success();
        assert_eq!(b.tokens(), 2);
        b.record_success();
        b.record_success();
        assert_eq!(b.tokens(), 3);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn refill_keeps_partial_progress() {
        let mut b = ReconnectBudget::new(3, PERIOD);
        for _ in 0..3 {
            b.record_failure();
        }
        // 1.5 periods: one token earned, half a period banked.
        tokio::time::advance(PERIOD + PERIOD / 2).await;
        assert_eq!(b.tokens(), 1);
        b.record_failure();
        assert_eq!(b.wait_time(), PERIOD / 2);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn full_bucket_does_not_bank_idle_time() {
        let mut b = ReconnectBudget::new(3, PERIOD);
        tokio::time::advance(PERIOD * 10).await;
        for _ in 0..3 {
            b.record_failure();
        }
        assert_eq!(b.wait_time(), PERIOD);
    }
}
