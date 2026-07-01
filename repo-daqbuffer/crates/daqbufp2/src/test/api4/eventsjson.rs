use crate::nodes::require_test_hosts_running;
use crate::test::api4::common::fetch_events_json;
use chrono::Utc;
use daqbuf_err::Error;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::AppendToUrl;
use netpod::Cluster;
use netpod::HostPort;
use netpod::ReqCtx;
use netpod::SfDbChannel;
use netpod::APP_JSON;
use query::api4::events::PlainEventsQuery;
use serde_json::Value as JsonValue;
use url::Url;

const TEST_BACKEND: &str = "testbackend-00";

fn make_query<S: Into<String>>(name: S, beg_date: &str, end_date: &str) -> Result<PlainEventsQuery, Error> {
    let channel = SfDbChannel::from_name(TEST_BACKEND, name);
    let beg_date = beg_date.parse()?;
    let end_date = end_date.parse()?;
    let range = NanoRange::from_date_time(beg_date, end_date);
    let query = PlainEventsQuery::new(channel, range).for_time_weighted_scalar();
    Ok(query)
}

#[test]
fn events_plain_json_00() -> Result<(), Error> {
    let fut = async {
        let rh = require_test_hosts_running()?;
        let cluster = &rh.cluster;
        let query = make_query(
            "test-gen-i32-dim0-v01",
            "1970-01-01T00:20:04.000Z",
            "1970-01-01T00:21:10.000Z",
        )?;
        // TODO
        let _jsv = fetch_events_json(query, cluster).await?;
        // let res: EventsDim0CollectorOutput<i32> = serde_json::from_value(jsv)?;
        // Tim-weighted uses one event before requested range:
        // assert_eq!(res.len(), 133);
        // assert_eq!(res.ts_anchor_sec(), 1203);
        Ok(())
    };
    taskrun::run(fut)
}

#[test]
fn events_plain_json_02_range_incomplete() -> Result<(), Error> {
    let fut = async {
        let rh = require_test_hosts_running()?;
        let cluster = &rh.cluster;
        let _jsv = events_plain_json(
            SfDbChannel::from_name(TEST_BACKEND, "test-gen-i32-dim0-v01"),
            "1970-01-03T23:59:55.000Z",
            "1970-01-04T00:00:01.000Z",
            cluster,
        )
        .await?;
        // let res: EventsDim0CollectorOutput<i32> = serde_json::from_value(jsv).unwrap();
        // assert_eq!(res.range_final(), false);
        // assert_eq!(res.timed_out(), false);
        Ok(())
    };
    taskrun::run(fut)
}

// TODO improve by a more information-rich return type.
async fn events_plain_json(
    channel: SfDbChannel,
    beg_date: &str,
    end_date: &str,
    cluster: &Cluster,
) -> Result<JsonValue, Error> {
    let ctx = ReqCtx::for_test();
    let t1 = Utc::now();
    let node0 = &cluster.nodes[0];
    let beg_date = beg_date.parse()?;
    let end_date = end_date.parse()?;
    let range = NanoRange::from_date_time(beg_date, end_date);
    let query = PlainEventsQuery::new(channel, range).for_time_weighted_scalar();
    let hp = HostPort::from_node(node0);
    let mut url = Url::parse(&format!("http://{}:{}/api/4/events", hp.host, hp.port))?;
    query.append_to_url(&mut url);
    let url = url;
    let res = httpclient::http_get(url, APP_JSON, &ctx).await?;
    let s = String::from_utf8_lossy(&res.body);
    let res: JsonValue = serde_json::from_str(&s)?;
    let pretty = serde_json::to_string_pretty(&res)?;
    info!("{pretty}");
    let t2 = chrono::Utc::now();
    let ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
    // TODO add timeout
    info!("time {} ms", ms);
    Ok(res)
}
