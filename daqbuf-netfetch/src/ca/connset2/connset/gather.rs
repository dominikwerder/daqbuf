use crate::ca::conn2::timeoutable::Timeoutable;
use futures::StreamExt;
use serde::Serialize;
use std::fmt;
use std::time::Duration;

pub const GATHER_CONCURRENCY: usize = 16;
pub const GATHER_TIMEOUT: Duration = Duration::from_millis(2000);

#[derive(Debug, Serialize)]
pub enum GatherError {
    Timeout,
    Comm(String),
}

impl fmt::Display for GatherError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GatherError::Timeout => write!(fmt, "timeout"),
            GatherError::Comm(e) => write!(fmt, "comm: {e}"),
        }
    }
}

/// Run `f` against each item concurrently and collect one outcome per key.
///
/// A dead `CaConn` stops draining its command channel, so an untimed send can block
/// forever and occupy the ConnSet command slot; every call is therefore bounded by
/// `timeout` and a failing target degrades to one `Err` entry instead of stalling the
/// whole gather. Results are sorted by key so repeated scrapes stay comparable.
pub async fn gather<K, C, T, E, F, Fut>(
    items: Vec<(K, C)>,
    concurrency: usize,
    timeout: Duration,
    f: F,
) -> Vec<(K, Result<T, GatherError>)>
where
    K: Ord,
    F: Fn(C) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: fmt::Display,
{
    let mut ret: Vec<_> = futures::stream::iter(items)
        .map(|(k, c)| {
            let fut = f(c);
            async move {
                let res = match fut.timeout(timeout).await {
                    Err(_) => Err(GatherError::Timeout),
                    Ok(Ok(x)) => Ok(x),
                    Ok(Err(e)) => Err(GatherError::Comm(e.to_string())),
                };
                (k, res)
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;
    ret.sort_by(|a, b| a.0.cmp(&b.0));
    ret
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Instant;
    use taskrun::tokio;

    async fn sleep_ok(d: Duration) -> Result<u64, String> {
        tokio::time::sleep(d).await;
        Ok(d.as_millis() as u64)
    }

    /// The gather must run the targets concurrently: a sequential loop over eight
    /// 100ms calls would take 800ms.
    #[test]
    fn gather_runs_concurrently() {
        let (elapsed, res) = taskrun::run(async move {
            let items: Vec<_> = (0..8u32).map(|i| (i, Duration::from_millis(100))).collect();
            let ts1 = Instant::now();
            let res = gather(items, GATHER_CONCURRENCY, Duration::from_millis(2000), sleep_ok).await;
            Ok::<_, err::Error>((ts1.elapsed(), res))
        })
        .unwrap();
        assert_eq!(res.len(), 8);
        assert!(res.iter().all(|(_, r)| matches!(r, Ok(100))), "{res:?}");
        assert!(elapsed < Duration::from_millis(400), "{elapsed:?}");
    }

    /// One wedged target must not take the others down with it.
    #[test]
    fn gather_isolates_timeout() {
        let res = taskrun::run(async move {
            let items = vec![
                (1u32, Duration::from_millis(10)),
                (2u32, Duration::from_millis(4000)),
                (3u32, Duration::from_millis(10)),
            ];
            Ok::<_, err::Error>(gather(items, GATHER_CONCURRENCY, Duration::from_millis(150), sleep_ok).await)
        })
        .unwrap();
        assert_eq!(res.len(), 3);
        assert!(matches!(res[0], (1, Ok(10))), "{res:?}");
        assert!(matches!(res[1], (2, Err(GatherError::Timeout))), "{res:?}");
        assert!(matches!(res[2], (3, Ok(10))), "{res:?}");
    }

    /// Every key is reported even when the call itself fails, so a caller can tell
    /// "no such connection" from "connection did not answer".
    #[test]
    fn gather_reports_call_error_per_key() {
        let res = taskrun::run(async move {
            let items = vec![(7u32, ()), (8u32, ())];
            Ok::<_, err::Error>(
                gather(items, GATHER_CONCURRENCY, Duration::from_millis(500), |()| async {
                    Err::<u64, _>("channel closed")
                })
                .await,
            )
        })
        .unwrap();
        assert_eq!(res.len(), 2);
        for (k, r) in res {
            match r {
                Err(GatherError::Comm(e)) => assert_eq!(e, "channel closed"),
                x => panic!("unexpected for {k}: {x:?}"),
            }
        }
    }
}
