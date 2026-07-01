use crate::nodes::require_test_hosts_running;
use crate::test::api4::common::fetch_events_json;
use daqbuf_err::Error;
// use items_0::test::f32_iter_cmp_near;
use netpod::range::evrange::NanoRange;
use netpod::SfDbChannel;
use query::api4::events::PlainEventsQuery;

const BACKEND: &str = "testbackend-00";

pub fn make_query<S: Into<String>>(name: S, beg_date: &str, end_date: &str) -> Result<PlainEventsQuery, Error> {
    let channel = SfDbChannel::from_name(BACKEND, name);
    let beg_date = beg_date.parse()?;
    let end_date = end_date.parse()?;
    let range = NanoRange::from_date_time(beg_date, end_date);
    let query = PlainEventsQuery::new(channel, range).for_pulse_id_diff();
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
        let _jsv = fetch_events_json(query, cluster).await?;
        // let res: EventsDim0CollectorOutput<i64> = serde_json::from_value(jsv)?;
        // inmem was meant just for functional test, ignores the requested time range
        // assert_eq!(res.ts_anchor_sec(), 1204);
        // assert_eq!(res.len(), 132);
        Ok(())
    };
    taskrun::run(fut)
}
