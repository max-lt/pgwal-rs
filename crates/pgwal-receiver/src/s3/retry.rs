//! Retries with exponential backoff, a time limit per attempt, and one for
//! the whole call.

use std::future::Future;
use std::io;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::Semaphore;
use tracing::warn;

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Retry {
    /// Attempts, the first included.
    pub attempts: u32,
    pub first_delay: Duration,
    pub max_delay: Duration,
    /// An attempt that takes longer fails, and counts as transient.
    pub attempt_timeout: Duration,
    /// No new attempt starts after this, from the start of the call.
    pub total: Duration,
}

impl Default for Retry {
    fn default() -> Self {
        Retry {
            attempts: 6,
            first_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            attempt_timeout: Duration::from_secs(60),
            total: Duration::from_secs(120),
        }
    }
}

/// Statuses that a later attempt can fix: overload, server faults,
/// timeouts, and a conditional write that ran into another one.
pub(crate) fn is_transient(status: u16, code: &str) -> bool {
    matches!(status, 408 | 429 | 500 | 502 | 503 | 504)
        || (status == 409 && code == "ConditionalRequestConflict")
}

impl Retry {
    /// The pause before attempt `n + 1`: doubling from `first_delay` up to
    /// `max_delay`, then a random point in its upper half, so many clients
    /// do not retry in step.
    fn delay(&self, n: u32) -> Duration {
        let full = self
            .first_delay
            .saturating_mul(1 << n.min(16))
            .min(self.max_delay);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.subsec_nanos());
        let half = full / 2;
        half + Duration::from_nanos(u64::from(nanos) % (half.as_nanos() as u64).max(1))
    }

    /// Runs `attempt` until it succeeds, holding one of `slots` per attempt.
    /// Every error of `attempt` counts as transient: the caller turns a
    /// final status into `Ok`.
    pub(crate) async fn run<T, F, Fut>(&self, slots: &Semaphore, attempt: F) -> io::Result<T>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = io::Result<T>>,
    {
        let start = Instant::now();
        let mut n = 0;
        loop {
            let result = {
                let _slot = slots.acquire().await.expect("the semaphore stays open");
                match tokio::time::timeout(self.attempt_timeout, attempt()).await {
                    Ok(result) => result,
                    Err(_) => Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("no answer in {:?}", self.attempt_timeout),
                    )),
                }
            };
            let error = match result {
                Ok(value) => return Ok(value),
                Err(error) => error,
            };
            n += 1;
            let delay = self.delay(n - 1);
            if n >= self.attempts || start.elapsed() + delay >= self.total {
                return Err(error);
            }
            warn!(attempt = n, retry_in = ?delay, "S3 request failed: {error}");
            tokio::time::sleep(delay).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delays_grow_and_stay_under_the_cap() {
        let retry = Retry::default();
        for n in 0..20 {
            let delay = retry.delay(n);
            assert!(delay <= retry.max_delay, "{delay:?}");
            let full = retry
                .first_delay
                .saturating_mul(1 << n.min(16))
                .min(retry.max_delay);
            assert!(delay >= full / 2, "{delay:?} < {full:?} / 2");
        }
    }

    #[test]
    fn only_some_statuses_are_transient() {
        assert!(is_transient(503, "SlowDown"));
        assert!(is_transient(409, "ConditionalRequestConflict"));
        assert!(!is_transient(409, "BucketNotEmpty"));
        assert!(!is_transient(412, "PreconditionFailed"));
        assert!(!is_transient(403, "SignatureDoesNotMatch"));
    }
}
