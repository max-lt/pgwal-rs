//! Retries with exponential backoff, a time limit per attempt, and one for
//! the whole call.

use std::future::Future;
use std::io;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::Semaphore;
use tracing::warn;

/// Attempts, the first included.
const ATTEMPTS: u32 = 6;
const FIRST_DELAY: Duration = Duration::from_millis(100);
const MAX_DELAY: Duration = Duration::from_secs(5);
/// An attempt that takes longer fails, and counts as transient.
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(60);
/// No new attempt starts after this, from the start of the call.
const TOTAL: Duration = Duration::from_secs(120);

/// Statuses that a later attempt can fix: overload, server faults,
/// timeouts, and a conditional write that ran into another one.
pub(crate) fn is_transient(status: u16, code: &str) -> bool {
    matches!(status, 408 | 429 | 500 | 502 | 503 | 504)
        || (status == 400 && code == "RequestTimeout")
        || (status == 409 && code == "ConditionalRequestConflict")
}

/// The pause before attempt `n + 1`: doubling from `FIRST_DELAY` up to
/// `MAX_DELAY`, then a random point in its upper half, so many clients do
/// not retry in step.
fn delay(n: u32) -> Duration {
    let full = FIRST_DELAY.saturating_mul(1 << n.min(16)).min(MAX_DELAY);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    let half = full / 2;
    half + Duration::from_nanos(u64::from(nanos) % (half.as_nanos() as u64).max(1))
}

/// Runs `attempt` until it succeeds, holding one of `slots` per attempt.
/// Retries every `Err` of `attempt`.
pub(crate) async fn run<T, F, Fut>(slots: &Semaphore, attempt: F) -> io::Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = io::Result<T>>,
{
    let start = Instant::now();
    let mut n = 0;
    loop {
        let result = {
            let _slot = slots.acquire().await.expect("the semaphore stays open");
            match tokio::time::timeout(ATTEMPT_TIMEOUT, attempt()).await {
                Ok(result) => result,
                Err(_) => Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("no answer in {ATTEMPT_TIMEOUT:?}"),
                )),
            }
        };
        let error = match result {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        n += 1;
        let delay = delay(n - 1);
        if n >= ATTEMPTS || start.elapsed() + delay >= TOTAL {
            return Err(error);
        }
        warn!(attempt = n, retry_in = ?delay, "S3 request failed: {error}");
        tokio::time::sleep(delay).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delays_double_up_to_the_cap() {
        let first = delay(0);
        assert!((50..=100).contains(&first.as_millis()), "{first:?}");
        let third = delay(2);
        assert!((200..=400).contains(&third.as_millis()), "{third:?}");
        let late = delay(30);
        assert!((2500..=5000).contains(&late.as_millis()), "{late:?}");
    }

    #[test]
    fn only_some_statuses_are_transient() {
        assert!(is_transient(503, "SlowDown"));
        assert!(is_transient(400, "RequestTimeout"));
        assert!(is_transient(409, "ConditionalRequestConflict"));
        assert!(!is_transient(400, "InvalidArgument"));
        assert!(!is_transient(409, "BucketNotEmpty"));
        assert!(!is_transient(412, "PreconditionFailed"));
        assert!(!is_transient(403, "SignatureDoesNotMatch"));
    }
}
