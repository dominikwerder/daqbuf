mod data_api_python;

use crate::nodes::require_test_hosts_running;
use daqbuf_err::Error;
use futures_util::Future;
use httpclient::http_post;
use netpod::APP_OCTET;
use netpod::ReqCtx;
use netpod::log::*;
use netpod::query::api1::Api1Query;
use netpod::query::api1::Api1Range;
use netpod::query::api1::ChannelTuple;
use parse::api1_parse;
use parse::api1_parse::Api1Frame;
use parse::api1_parse::Api1ScalarType;
use std::fmt;
use url::Url;

const TEST_BACKEND: &str = "testbackend-00";

fn testrun<T, F>(fut: F) -> Result<T, Error>
where
    F: Future<Output = Result<T, Error>>,
{
    taskrun::run(fut)
}

fn is_monotonic_strict<I>(it: I) -> bool
where
    I: Iterator,
    <I as Iterator>::Item: PartialOrd + fmt::Debug,
{
    let mut last = None;
    for x in it {
        if let Some(last) = last.as_ref() {
            if x <= *last {
                return false;
            }
        }
        last = Some(x);
    }
    true
}

#[test]
fn test_is_monitonic_strict() {
    assert_eq!(is_monotonic_strict([1, 2, 3].iter()), true);
    assert_eq!(is_monotonic_strict([1, 2, 2].iter()), false);
}

#[test]
fn events_f64_plain() -> Result<(), Error> {
    // TODO re-enable with in-memory generated config and event data.
    if true {
        return Ok(());
    }
    let fut = async {
        let ctx = ReqCtx::for_test();
        let rh = require_test_hosts_running()?;
        let cluster = &rh.cluster;
        let node = &cluster.nodes[0];
        let url: Url = format!("http://{}:{}/api/1/query", node.host, node.port).parse()?;
        let accept = APP_OCTET;
        let range = Api1Range::new(
            "1970-01-01T00:00:00Z".try_into().map_err(Error::from_string)?,
            "1970-01-01T00:01:00Z".try_into().map_err(Error::from_string)?,
        )
        .map_err(Error::from_string)?;
        // TODO the channel list needs to get pre-processed to check for backend prefix!
        let ch = ChannelTuple::new(TEST_BACKEND.into(), "test-gen-i32-dim0-v01".into());
        let qu = Api1Query::new(range, vec![ch]);
        let body = serde_json::to_string(&qu)?;
        let buf = http_post(url, accept, body.into(), &ctx).await?;
        eprintln!("body received: {}", buf.len());
        match api1_parse::api1_frames::<parse::nom::error::VerboseError<_>>(&buf) {
            Ok((_, frames)) => {
                debug!("FRAMES LEN: {}", frames.len());
                assert_eq!(frames.len(), 121);
                if let Api1Frame::Header(header) = &frames[0] {
                    if let Api1ScalarType::I32 = header.header().ty() {
                    } else {
                        panic!("bad scalar type")
                    }
                } else {
                    panic!("bad header")
                }
                let tss: Vec<_> = frames
                    .iter()
                    .filter_map(|f| match f {
                        api1_parse::Api1Frame::Data(d) => Some(d.ts()),
                        _ => None,
                    })
                    .collect();
                let pulses: Vec<_> = frames
                    .iter()
                    .filter_map(|f| match f {
                        api1_parse::Api1Frame::Data(d) => Some(d.pulse()),
                        _ => None,
                    })
                    .collect();
                let values: Vec<_> = frames
                    .iter()
                    .filter_map(|f| match f {
                        api1_parse::Api1Frame::Data(d) => {
                            let val = i32::from_be_bytes(d.data().try_into().unwrap());
                            Some(val)
                        }
                        _ => None,
                    })
                    .collect();
                assert_eq!(is_monotonic_strict(tss.iter()), true);
                assert_eq!(is_monotonic_strict(pulses.iter()), true);
                assert_eq!(is_monotonic_strict(values.iter()), true);
                for &val in &values {
                    assert!(val >= 0);
                    assert!(val < 120);
                }
            }
            Err(e) => {
                error!("can not parse result: {e}");
                panic!()
            }
        };
        Ok(())
    };
    testrun(fut)?;
    Ok(())
}
