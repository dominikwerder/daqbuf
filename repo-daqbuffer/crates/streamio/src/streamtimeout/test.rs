use super::StreamTimeout;
use futures_util::StreamExt;
use std::time::Duration;
use std::time::Instant;
use streams::streamtimeout::TimeoutableStream;

async fn stream_timeout_inner() -> Result<(), u8> {
    let stream = futures_util::stream::iter((0..500).collect::<Vec<_>>());
    let stream = stream.then({
        let mut i = 0;
        move |x| {
            i += 1;
            let dur = if i % 5 == 0 { 500 } else { 0 };
            async move {
                tokio::time::sleep(Duration::from_millis(dur)).await;
                x
            }
        }
    });
    let stream = stream.inspect(|_| {
        // eprintln!("A see {x:?}");
    });
    let timeout_provider = StreamTimeout::arced();
    let ivl = Duration::from_millis(200);
    let stream = TimeoutableStream::new(ivl, timeout_provider, stream);
    let stream = stream.inspect({
        let mut tsl = Instant::now();
        move |x| match x {
            Some(x) => {
                let tsnow = Instant::now();
                let dt = tsnow.saturating_duration_since(tsl).as_secs_f32() * 1e3;
                eprintln!("B see {x:?}  {dt:7.2}");
                tsl = tsnow;
            }
            None => {
                eprintln!("B see None");
            }
        }
    });
    stream.count().await;
    Ok(())
}

#[test]
fn stream_timeout() {
    taskrun::run(stream_timeout_inner()).unwrap()
}
