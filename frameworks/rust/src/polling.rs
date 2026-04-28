//! Bounded polling loop helper — wait until a predicate returns Ok(true)
//! or the deadline passes. Used by tools that gate progress on external
//! state (e.g. `checkpoint` waiting for gate files to flip predicate_met).
//!
//! Deliberately simple: synchronous and async variants, fixed interval, no
//! exponential backoff. Tools that need fancier behavior should compose
//! their own.

use std::time::{Duration, Instant};

/// Outcome of a [`poll_until`] / [`poll_until_async`] call.
#[derive(Debug, PartialEq, Eq)]
pub enum PollOutcome {
    /// Predicate returned true within the timeout.
    Ready,
    /// Deadline elapsed before the predicate returned true.
    TimedOut,
}

/// Synchronously call `predicate` every `interval` until it returns
/// `Ok(true)` or `timeout` elapses. Predicate errors are propagated and
/// abort the loop immediately.
pub fn poll_until<F, E>(
    mut predicate: F,
    interval: Duration,
    timeout: Duration,
) -> Result<PollOutcome, E>
where
    F: FnMut() -> Result<bool, E>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if predicate()? {
            return Ok(PollOutcome::Ready);
        }
        if Instant::now() >= deadline {
            return Ok(PollOutcome::TimedOut);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        std::thread::sleep(interval.min(remaining));
    }
}

/// Async variant — uses `tokio::time::sleep`, so callers must run inside a
/// tokio runtime.
pub async fn poll_until_async<F, Fut, E>(
    mut predicate: F,
    interval: Duration,
    timeout: Duration,
) -> Result<PollOutcome, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<bool, E>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if predicate().await? {
            return Ok(PollOutcome::Ready);
        }
        if Instant::now() >= deadline {
            return Ok(PollOutcome::TimedOut);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        tokio::time::sleep(interval.min(remaining)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[test] fn returns_ready_when_predicate_succeeds() {
        let count = AtomicU32::new(0);
        let r = poll_until::<_, std::convert::Infallible>(
            || {
                let n = count.fetch_add(1, Ordering::SeqCst);
                Ok(n >= 2)
            },
            Duration::from_millis(5),
            Duration::from_secs(1),
        ).unwrap();
        assert_eq!(r, PollOutcome::Ready);
    }

    #[test] fn returns_timeout_when_predicate_never_succeeds() {
        let r = poll_until::<_, std::convert::Infallible>(
            || Ok(false),
            Duration::from_millis(5),
            Duration::from_millis(30),
        ).unwrap();
        assert_eq!(r, PollOutcome::TimedOut);
    }

    #[test] fn predicate_error_aborts_loop() {
        let r: Result<PollOutcome, &'static str> = poll_until(
            || Err("boom"),
            Duration::from_millis(5),
            Duration::from_secs(1),
        );
        assert_eq!(r, Err("boom"));
    }

    #[tokio::test] async fn async_returns_ready_when_predicate_succeeds() {
        let count = AtomicU32::new(0);
        let r = poll_until_async::<_, _, std::convert::Infallible>(
            || async {
                let n = count.fetch_add(1, Ordering::SeqCst);
                Ok(n >= 2)
            },
            Duration::from_millis(5),
            Duration::from_secs(1),
        ).await.unwrap();
        assert_eq!(r, PollOutcome::Ready);
    }

    #[tokio::test] async fn async_returns_timeout() {
        let r = poll_until_async::<_, _, std::convert::Infallible>(
            || async { Ok(false) },
            Duration::from_millis(5),
            Duration::from_millis(30),
        ).await.unwrap();
        assert_eq!(r, PollOutcome::TimedOut);
    }
}
