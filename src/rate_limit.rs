//! Token-bucket rate limiter for outbound Prusa HTTPS calls.
//!
//! Policy (from spec):
//! - Bucket cap: 3, initial state full.
//! - Refill: 1 token / 60s.
//! - `acquire()` reserves 1 token; if empty, parks until refill.
//! - Caller reports the outcome: `Outcome::Success` (refund), `Outcome::ClientError` (refund),
//!   `Outcome::ServerOrNetworkError` (consume).

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Success,
    ClientError,           // 4xx
    ServerOrNetworkError,  // 5xx, DNS, timeout, TLS, connect refused
}

#[derive(Debug)]
pub struct RateLimiter {
    inner: Arc<Mutex<Inner>>,
    cap: u32,
    refill_period: Duration,
}

#[derive(Debug)]
struct Inner {
    tokens: u32,
    last_refill: Instant,
}

pub struct Permit<'a> {
    limiter: &'a RateLimiter,
    consumed: bool,
}

impl<'a> Permit<'a> {
    pub async fn complete(mut self, outcome: Outcome) {
        let consume = matches!(outcome, Outcome::ServerOrNetworkError);
        if !consume {
            self.limiter.refund().await;
        }
        self.consumed = true;
    }
}

impl<'a> Drop for Permit<'a> {
    fn drop(&mut self) {
        if !self.consumed {
            // Treat dropped permits as consumed (defensive; caller should always call complete).
            tracing::warn!("rate-limit permit dropped without complete(); treated as consumed");
        }
    }
}

impl RateLimiter {
    pub fn new(cap: u32, refill_period: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner { tokens: cap, last_refill: Instant::now() })),
            cap,
            refill_period,
        }
    }

    pub async fn acquire(&self) -> Permit<'_> {
        loop {
            let wait = {
                let mut inner = self.inner.lock().await;
                self.tick(&mut inner);
                if inner.tokens > 0 {
                    inner.tokens -= 1;
                    return Permit { limiter: self, consumed: false };
                }
                let elapsed = inner.last_refill.elapsed();
                self.refill_period.saturating_sub(elapsed)
            };
            tracing::warn!(wait_secs = wait.as_secs(), "rate-limit bucket empty; waiting for refill");
            tokio::time::sleep(wait).await;
        }
    }

    async fn refund(&self) {
        let mut inner = self.inner.lock().await;
        if inner.tokens < self.cap {
            inner.tokens += 1;
        }
    }

    fn tick(&self, inner: &mut Inner) {
        let elapsed = inner.last_refill.elapsed();
        if elapsed >= self.refill_period && inner.tokens < self.cap {
            let earned = (elapsed.as_secs() / self.refill_period.as_secs()) as u32;
            inner.tokens = (inner.tokens + earned).min(self.cap);
            inner.last_refill = Instant::now();
        }
    }

    #[cfg(test)]
    pub async fn tokens(&self) -> u32 {
        let mut inner = self.inner.lock().await;
        self.tick(&mut inner);
        inner.tokens
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn success_does_not_consume_tokens() {
        let rl = RateLimiter::new(3, Duration::from_secs(60));
        for _ in 0..10 {
            rl.acquire().await.complete(Outcome::Success).await;
        }
        assert_eq!(rl.tokens().await, 3);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn client_error_does_not_consume_tokens() {
        let rl = RateLimiter::new(3, Duration::from_secs(60));
        rl.acquire().await.complete(Outcome::ClientError).await;
        assert_eq!(rl.tokens().await, 3);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn server_error_drains_bucket() {
        let rl = RateLimiter::new(3, Duration::from_secs(60));
        for _ in 0..3 {
            rl.acquire().await.complete(Outcome::ServerOrNetworkError).await;
        }
        assert_eq!(rl.tokens().await, 0);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn refill_restores_tokens_over_time() {
        let rl = RateLimiter::new(3, Duration::from_secs(60));
        for _ in 0..3 {
            rl.acquire().await.complete(Outcome::ServerOrNetworkError).await;
        }
        assert_eq!(rl.tokens().await, 0);
        tokio::time::advance(Duration::from_secs(60)).await;
        assert_eq!(rl.tokens().await, 1);
        tokio::time::advance(Duration::from_secs(60)).await;
        assert_eq!(rl.tokens().await, 2);
    }
}
