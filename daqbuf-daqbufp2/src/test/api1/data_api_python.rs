use crate::nodes::require_test_hosts_running;
use chrono::Utc;
use daqbuf_err::Error;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::timeunits::MS;
use netpod::Cluster;
use netpod::HostPort;
use netpod::ReqCtx;
use netpod::SfDbChannel;
use netpod::APP_JSON;
use netpod::DATETIME_FMT_3MS;
use parse::api1_parse::api1_frames;
use parse::api1_parse::Api1Frame;
use parse::api1_parse::Api1ScalarType;
use url::Url;

const TEST_BACKEND: &str = "testbackend-00";

// Fetches all data, not streaming, meant for basic test cases that fit in memory.
async fn fetch_data_api_python_blob(
    channels: Vec<SfDbChannel>,
    beg_date: &str,
    end_date: &str,
    cluster: &Cluster,
) -> Result<Vec<u8>, Error> {
    let ctx = ReqCtx::for_test();
    let t1 = Utc::now();
    let node0 = &cluster.nodes[0];
    let beg_date = beg_date.parse()?;
    let end_date = end_date.parse()?;
    let _range = NanoRange::from_date_time(beg_date, end_date);
    let start_date = beg_date.format(DATETIME_FMT_3MS).to_string();
    let end_date = end_date.format(DATETIME_FMT_3MS).to_string();
    let query = serde_json::json!({
        "range": {
            "type": "date",
            "startDate": start_date,
            "endDate": end_date,
        },
        "channels": channels.iter().map(|x| x.name()).collect::<Vec<_>>(),
        "create_errors": "nodenet_parse_query",
    });
    let query_str = serde_json::to_string_pretty(&query)?;
    let hp = HostPort::from_node(node0);
    let url = Url::parse(&format!("http://{}:{}/api/1/query", hp.host, hp.port))?;
    info!("http get {}", url);
    let buf = httpclient::http_post(url, APP_JSON, query_str, &ctx).await?;
    let t2 = chrono::Utc::now();
    let ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
    // TODO add timeout
    info!("time {} ms  body len {}", ms, buf.len());
    Ok(buf.into())
}

#[test]
fn api3_hdf_dim0_00() -> Result<(), Error> {
    let fut = async {
        let rh = require_test_hosts_running()?;
        let cluster = &rh.cluster;
        let jsv = fetch_data_api_python_blob(
            vec![SfDbChannel::from_name(TEST_BACKEND, "test-gen-i32-dim0-v00")],
            "1970-01-01T00:20:04.000Z",
            "1970-01-01T00:21:10.000Z",
            cluster,
        )
        .await?;
        use nom::error::VerboseError;
        use parse::nom;
        if false {
            // Uses the default error type, but not unwrapped:
            let _x: nom::IResult<_, _> = api1_frames(&jsv);
        }
        if false {
            // Default error and unwrapped:
            let (_, _) = api1_frames::<nom::error::Error<_>>(&jsv).unwrap();
        }
        let (_, frames) = api1_frames::<VerboseError<_>>(&jsv).unwrap();
        if let Api1Frame::Header(header) = frames.get(0).expect("frame") {
            assert_eq!(header.header().ty(), Api1ScalarType::I32);
        } else {
            panic!("expect header frame");
        }
        for (i, frame) in frames[1..].iter().enumerate() {
            if let Api1Frame::Data(data) = frame {
                assert_eq!(data.ts() / MS, 1000 * (60 * 20 + 4 + i as u64));
                eprintln!("ts {}", data.ts() / MS);
            } else {
                panic!("expect data frame");
            }
        }
        assert_eq!(frames.len(), 67);
        Ok(())
    };
    taskrun::run(fut)
}
