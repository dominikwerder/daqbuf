use super::*;
use crate::metrics::types::MetricsPrometheusShort;

#[derive(Clone)]
struct Conn2TestCtrls {
    metrics: Vec<String>,
}

impl Conn2TestCtrls {
    fn new() -> Self {
        let mut connset = stats::mett::ConnSet2Metrics::new();
        connset.ca_conn_create().add(2);
        let mut conn = stats::mett::CaConn2Metrics::new();
        conn.tcp_connected().add(2);
        conn.connected().proto().tcp_recv_bytes().add(1234);
        connset.ca_conn().ingest(conn.take_and_reset());
        let metrics = MetricsPrometheusShort::from(&connset);
        Self {
            metrics: metrics.into_flatten_prometheus(),
        }
    }
}

impl Conn2Ctrls for Conn2TestCtrls {
    fn connection_list_get_v1(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<ConnectionListV1, Box<dyn std::error::Error>>> + Send>> {
        unimplemented!()
    }

    fn channels_for_addr_v1(
        &self,
        _addr: SocketAddrV4,
    ) -> Pin<Box<dyn Future<Output = Result<ChannelsForAddrInfoV1, Box<dyn std::error::Error>>> + Send>> {
        unimplemented!()
    }

    fn channels_for_addr_v2(
        &self,
        _addr: SocketAddrV4,
        _name: String,
    ) -> Pin<Box<dyn Future<Output = Result<ChannelsForAddrInfoV2, Box<dyn std::error::Error>>> + Send>> {
        unimplemented!()
    }

    fn cmd_dyn_v1(
        &self,
        _cmd: String,
    ) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, Box<dyn std::error::Error>>> + Send>> {
        unimplemented!()
    }

    fn channel_add_v1(
        &self,
        _name: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error>>> + Send>> {
        unimplemented!()
    }

    fn channel_remove_v1(
        &self,
        _name: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error>>> + Send>> {
        unimplemented!()
    }

    fn status_light_v1(
        &self,
        sel: status_v1::ChannelSelector,
    ) -> Pin<Box<dyn Future<Output = Result<status_v1::StatusLight, Box<dyn std::error::Error>>> + Send>> {
        let ret = status_v1::StatusLight {
            ts: "2026-09-09T00:00:00Z".into(),
            ingest_name: format!("ch={:?} addr={:?}", sel.channel_pattern(), sel.addr_pattern()),
            conn_count_total: 2,
            conn_count_matched: 1,
            connset_channel_count_total: 2,
            connset_channels: vec![
                crate::ca::connset2::connset::channels::channel::ChannelStatusLight {
                    name: "SEARCHING:CHANNEL".into(),
                    state: "AddrSearch".into(),
                    backoff_i: 0,
                    backoff_remaining_ms: None,
                    addr: None,
                },
                crate::ca::connset2::connset::channels::channel::ChannelStatusLight {
                    name: "SOME:CHANNEL".into(),
                    state: "Observing".into(),
                    backoff_i: 0,
                    backoff_remaining_ms: None,
                    addr: Some("10.0.0.5:5064".into()),
                },
            ],
            conns: vec![status_v1::StatusLightConn {
                addr: "10.0.0.5:5064".into(),
                state: "Connected".into(),
                connected_state: Some("ActiveCa".into()),
                activeca_state: Some("Running".into()),
                channel_count_total: 1,
                channels: vec![status_v1::StatusLightChannel {
                    name: "SOME:CHANNEL".into(),
                    cid: 7,
                    state: "Running".into(),
                    state_elapsed_ms: 1234,
                    proto_inp_buf_len: 0,
                    outbuf_len: 0,
                    enum_variants_len: None,
                    event_add_res_cnt: 42,
                }],
                error: None,
            }],
        };
        Box::pin(async move { Ok(ret) })
    }

    fn status_full_v1(
        &self,
        sel: status_v1::ChannelSelector,
    ) -> Pin<Box<dyn Future<Output = Result<status_v1::StatusFull, Box<dyn std::error::Error>>> + Send>> {
        let ret = status_v1::StatusFull {
            ts: "2026-09-09T00:00:00Z".into(),
            ingest_name: format!("ch={:?} addr={:?}", sel.channel_pattern(), sel.addr_pattern()),
            conn_count_total: 2,
            conn_count_matched: 1,
            connset_channel_count_total: 0,
            connset_channels: Vec::new(),
            conns: vec![status_v1::StatusFullConn {
                addr: "10.0.0.5:5064".into(),
                state: "Connected".into(),
                connected_state: Some("ActiveCa".into()),
                activeca_state: Some("Running".into()),
                socket_state: None,
                channel_count_total: 1,
                channels: Vec::new(),
                error: None,
            }],
        };
        Box::pin(async move { Ok(ret) })
    }

    fn get_metrics(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<MetricsPrometheusShort, Box<dyn std::error::Error>>> + Send>> {
        let ret = MetricsPrometheusShort::from_flatten_prometheus(self.metrics.clone());
        Box::pin(async move { Ok(ret) })
    }
}

struct TestCaIngestCtrls {
    // None for v1 daemon, otherwise v2
    conn2: Option<Conn2TestCtrls>,
    daemon: stats::mett::DaemonMetrics,
}

impl TestCaIngestCtrls {
    fn new(with_conn2: bool) -> Self {
        let mut daemon = stats::mett::DaemonMetrics::new();
        daemon.handle_event().add(11);
        Self {
            conn2: if with_conn2 { Some(Conn2TestCtrls::new()) } else { None },
            daemon,
        }
    }
}

impl CaIngestCtrls for TestCaIngestCtrls {
    fn timer_tick(&self, _v: u32) -> Box<dyn Future<Output = u32>> {
        unimplemented!()
    }

    fn get_metrics(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<MetricsPrometheusShort, Box<dyn std::error::Error>>> + Send>> {
        let ret = MetricsPrometheusShort::from(&self.daemon);
        Box::pin(async move { Ok(ret) })
    }

    fn channel_add(
        &self,
        _conf: ChannelConfig,
    ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error>>> + Send>> {
        unimplemented!()
    }

    fn channel_remove(
        &self,
        _name: ChannelName,
    ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error>>> + Send>> {
        unimplemented!()
    }

    fn config_reload(&self) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error>>> + Send>> {
        unimplemented!()
    }

    fn shutdown(&self) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error>>> + Send>> {
        unimplemented!()
    }

    fn channel_states(
        &self,
        _name: String,
        _limit: u64,
    ) -> Pin<Box<dyn Future<Output = Result<ChannelStatusesResponse, Box<dyn std::error::Error>>> + Send>> {
        unimplemented!()
    }

    fn conn2_ctrls(&self) -> Pin<Box<dyn Future<Output = Option<Box<dyn Conn2Ctrls>>> + Send>> {
        let x = self.conn2.clone();
        Box::pin(async move { x.map(|x| Box::new(x) as Box<dyn Conn2Ctrls>) })
    }
}

struct TestPostIngestCtrls {}

impl PostIngestCtrls for TestPostIngestCtrls {
    fn resources(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Arc<RoutesResources>, Box<dyn std::error::Error>>> + Send>> {
        unimplemented!()
    }
}

async fn scrape(with_conn2: bool, uri: &str) -> (StatusCode, String) {
    use tower::ServiceExt;
    let router = make_routes(
        Arc::new(TestCaIngestCtrls::new(with_conn2)),
        Arc::new(TestPostIngestCtrls {}),
    );
    let req = Request::builder().uri(uri).body(axum::body::Body::empty()).unwrap();
    let res = router.oneshot(req).await.unwrap();
    let status = res.status();
    let body = axum::body::to_bytes(res.into_body(), 1 << 20).await.unwrap();
    (status, String::from_utf8_lossy(&body).into_owned())
}

fn scrape_blocking(with_conn2: bool, uri: &'static str) -> (StatusCode, String) {
    taskrun::run(async move { Ok::<_, err::Error>(scrape(with_conn2, uri).await) }).unwrap()
}

#[test]
fn scrape_metrics_v1_only() {
    let (status, body) = scrape_blocking(false, "/daqingest/metrics");
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("daemon_handle_event 11\n"), "{body}");
    assert!(!body.contains("daemon2_"), "{body}");
}

#[test]
fn scrape_metrics_with_conn2() {
    let (status, body) = scrape_blocking(true, "/daqingest/metrics");
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("daemon_handle_event 11\n"), "{body}");
    assert!(body.contains("daemon2_connset_ca_conn_create 2\n"), "{body}");
    assert!(body.contains("daemon2_connset_ca_conn_tcp_connected 2\n"), "{body}");
    assert!(
        body.contains("daemon2_connset_ca_conn_connected_proto_tcp_recv_bytes 1234\n"),
        "{body}"
    );
}

#[test]
fn scrape_metrics_trailing_slash() {
    let (status, body) = scrape_blocking(false, "/daqingest/metrics/");
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("daemon_handle_event 11\n"), "{body}");
}

#[test]
fn scrape_metrics_conn2_route() {
    let (status, body) = scrape_blocking(true, "/daqingest/private/conn2/metrics");
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("daemon2_connset_ca_conn_create 2\n"), "{body}");
    assert!(!body.contains("daemon_handle_event"), "{body}");
}

#[test]
fn admin_status_light() {
    let (status, body) = scrape_blocking(true, "/daqingest/admin/status/light");
    assert_eq!(status, StatusCode::OK, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["conn_count_total"], 2);
    assert_eq!(v["conns"][0]["addr"], "10.0.0.5:5064");
    assert_eq!(v["conns"][0]["activeca_state"], "Running");
    assert_eq!(v["conns"][0]["channels"][0]["name"], "SOME:CHANNEL");
    assert_eq!(v["conns"][0]["channels"][0]["state"], "Running");
    assert_eq!(v["conns"][0]["channels"][0]["event_add_res_cnt"], 42);
    assert_eq!(v["connset_channels"][0]["name"], "SEARCHING:CHANNEL");
    assert_eq!(v["connset_channels"][0]["state"], "AddrSearch");
    let connset_names: Vec<_> = v["connset_channels"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x["name"].as_str().unwrap())
        .collect();
    assert!(connset_names.contains(&"SOME:CHANNEL"), "{connset_names:?}");
    let conn_names: Vec<_> = v["conns"][0]["channels"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x["name"].as_str().unwrap())
        .collect();
    assert!(conn_names.contains(&"SOME:CHANNEL"), "{conn_names:?}");
}

#[test]
fn admin_status_full() {
    let (status, body) = scrape_blocking(true, "/daqingest/admin/status/full");
    assert_eq!(status, StatusCode::OK, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["conns"][0]["addr"], "10.0.0.5:5064");
}

#[test]
fn admin_status_default_selectors_match_all() {
    let (status, body) = scrape_blocking(true, "/daqingest/admin/status/light");
    assert_eq!(status, StatusCode::OK, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["ingest_name"], "ch=\"\" addr=\"\"");
}

#[test]
fn admin_status_passes_selectors_through() {
    let (status, body) = scrape_blocking(
        true,
        "/daqingest/admin/status/light?channel_regex=ABC&addr_regex=10%5C.",
    );
    assert_eq!(status, StatusCode::OK, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["ingest_name"], "ch=\"ABC\" addr=\"10\\\\.\"");
}

#[test]
fn admin_status_bad_regex_is_400() {
    let (status, body) = scrape_blocking(true, "/daqingest/admin/status/light?channel_regex=%5B");
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["kind"], "bad-regex");
}

#[test]
fn admin_status_without_conn2_is_503() {
    let (status, body) = scrape_blocking(false, "/daqingest/admin/status/light");
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["kind"], "conn2-not-active");
}

#[test]
fn openapi_json_contains_admin_status() {
    let (status, body) = scrape_blocking(false, "/daqingest/api-docs/openapi.json");
    assert_eq!(status, StatusCode::OK, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let light = &v["paths"]["/daqingest/admin/status/light"]["get"];
    assert!(light.is_object(), "{body}");
    assert!(v["paths"]["/daqingest/admin/status/full"]["get"].is_object(), "{body}");
    let params = light["parameters"].as_array().unwrap();
    let names: Vec<_> = params.iter().map(|x| x["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"channel_regex"), "{params:?}");
    assert!(names.contains(&"addr_regex"), "{params:?}");
    assert!(params.iter().all(|x| x["in"] == "query"), "{params:?}");
    assert!(params.iter().all(|x| x["required"] == false), "{params:?}");
    assert!(v["components"]["schemas"]["StatusLight"].is_object(), "{body}");
    assert!(v["components"]["schemas"]["StatusLightChannel"].is_object(), "{body}");
}

/// `conns[].channels[].handler` used to be an opaque `Object` in the OpenAPI schema. This
/// pins that the generated tree now carries real types for `state_dt`/`dwell_score` down
/// through the nested sub-state-machines, and that the several module-local `State` (and
/// `StateDirection`) enums landed under distinct component names instead of silently
/// colliding and overwriting each other.
#[test]
fn openapi_handler_state_tree_is_typed() {
    let (status, body) = scrape_blocking(false, "/daqingest/api-docs/openapi.json");
    assert_eq!(status, StatusCode::OK, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let schemas = &v["components"]["schemas"];

    let handler = &schemas["StatusFullChannel"]["properties"]["handler"];
    assert_eq!(
        handler["$ref"], "#/components/schemas/ChannelHandlerSerde",
        "handler is no longer typed as a concrete schema: {body}"
    );

    let fetch_polling = &schemas["FetchPollingStateSerde"];
    assert!(fetch_polling.is_object(), "FetchPollingStateSerde missing: {body}");
    let send_req = fetch_polling["oneOf"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["properties"]["ty"]["enum"] == serde_json::json!(["SendReq"]))
        .expect("SendReq variant missing");
    assert_eq!(
        send_req["properties"]["co"]["$ref"], "#/components/schemas/fetchpolling.PollStateDirection",
        "{body}"
    );

    let fetch_polling_struct = &schemas["FetchPollingSerde"]["properties"];
    assert_eq!(fetch_polling_struct["state_dt"]["type"], "string", "{body}");
    assert_ne!(fetch_polling_struct["dwell_score"], serde_json::json!({}), "{body}");
    assert!(fetch_polling_struct["dwell_score"]["type"].is_array(), "{body}");

    // The two module-local `StateDirection` enums must not have collided under one name.
    assert_eq!(
        schemas["fetchpolling.PollStateDirection"]["enum"],
        serde_json::json!(["None", "DoNothing"]),
        "{body}"
    );
    assert_eq!(
        schemas["fetchmonitoring.MonitorStateDirection"]["enum"],
        serde_json::json!(["None", "Disable", "Enable", "Closing"]),
        "{body}"
    );

    // Every module-local `State` enum must have its own distinct schema name.
    for name in [
        "StateSerde",
        "CreateStateSerde",
        "ReadEnumStateSerde",
        "RunningStateSerde",
        "FetchmpxStateSerde",
        "FetchPollingStateSerde",
        "FetchMonitoringStateSerde",
    ] {
        assert!(schemas[name].is_object(), "{name} missing from schema: {body}");
    }
}

#[test]
fn swagger_ui_served() {
    let (status, body) = scrape_blocking(false, "/daqingest/swagger-ui/");
    assert_eq!(status, StatusCode::OK, "{body}");
}
