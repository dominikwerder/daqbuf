use std::fmt;
use std::time::Duration;
use taskrun::tokio;

#[derive(Debug)]
pub struct TimeoutError {}

impl fmt::Display for TimeoutError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, fmt)
    }
}

pub trait Timeoutable<Fut>
where
    Fut: Future,
{
    fn timeout(self, timeout: Duration) -> impl Future<Output = Result<Fut::Output, TimeoutError>>;
}

impl<Fut> Timeoutable<Fut> for Fut
where
    Fut: Future,
{
    fn timeout(self, timeout: Duration) -> impl Future<Output = Result<<Fut as Future>::Output, TimeoutError>> {
        use futures::TryFutureExt;
        tokio::time::timeout(timeout, self).map_err(|_| TimeoutError {})
    }
}
