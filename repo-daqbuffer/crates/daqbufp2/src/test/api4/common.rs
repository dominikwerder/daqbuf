use chrono::Utc;
use daqbuf_err::Error;
use netpod::log::*;
use netpod::AppendToUrl;
use netpod::Cluster;
use netpod::HostPort;
use netpod::ReqCtx;
use netpod::APP_JSON;
use query::api4::binned::BinnedQuery;
use query::api4::events::PlainEventsQuery;
use serde_json::Value as JsonValue;
use url::Url;

// TODO improve by a more information-rich return type.
pub async fn fetch_events_json(query: PlainEventsQuery, cluster: &Cluster) -> Result<JsonValue, Error> {
    let ctx = ReqCtx::for_test();
    let t1 = Utc::now();
    let node0 = &cluster.nodes[0];
    let hp = HostPort::from_node(node0);
    let mut url = Url::parse(&format!("http://{}:{}/api/4/events", hp.host, hp.port))?;
    query.append_to_url(&mut url);
    let url = url;
    let res = httpclient::http_get(url, APP_JSON, &ctx).await?;
    let s = String::from_utf8_lossy(&res.body);
    let res: JsonValue = serde_json::from_str(&s)?;
    let pretty = serde_json::to_string_pretty(&res)?;
    debug!("fetch_binned_json pretty: {pretty}");
    let t2 = chrono::Utc::now();
    let ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
    // TODO add timeout
    debug!("time {} ms", ms);
    Ok(res)
}

// TODO improve by a more information-rich return type.
#[allow(unused)]
pub async fn fetch_binned_json(query: BinnedQuery, cluster: &Cluster) -> Result<JsonValue, Error> {
    let ctx = ReqCtx::for_test();
    let t1 = Utc::now();
    let node0 = &cluster.nodes[0];
    let hp = HostPort::from_node(node0);
    let mut url = Url::parse(&format!("http://{}:{}/api/4/binned", hp.host, hp.port))?;
    query.append_to_url(&mut url);
    let url = url;
    let res = httpclient::http_get(url, APP_JSON, &ctx).await?;
    let s = String::from_utf8_lossy(&res.body);
    let res: JsonValue = serde_json::from_str(&s)?;
    let pretty = serde_json::to_string_pretty(&res)?;
    debug!("fetch_binned_json pretty: {pretty}");
    let t2 = chrono::Utc::now();
    let ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
    // TODO add timeout
    debug!("time {} ms", ms);
    Ok(res)
}
