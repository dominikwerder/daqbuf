use crate::ca::conn2::conn;
use crate::ca::conn2::conn::StatusDetail;
use crate::ca::conn2::conn::StatusSel;
use crate::ca::conn2::conn::activeca;
use crate::ca::conn2::conn::channelheap;
use crate::ca::conn2::conn::connected;
use crate::ca::connset2::connset::ConnsetChannels;
use crate::ca::connset2::connset::StatusV1Res;
use crate::ca::connset2::connset::channels::channel::ChannelInfo;
use crate::ca::connset2::connset::channels::channel::ChannelStatusLight;
use crate::ca::connset2::connset::gather::GatherError;
use crate::metrics::CaIngestCtrls;
use crate::metrics::Conn2Ctrls;
use axum::http;
use regex::Regex;
use serde::Deserialize;
use serde::Serialize;
use std::net::SocketAddrV4;
use std::sync::Arc;
use utoipa::IntoParams;
use utoipa::ToSchema;

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct StatusQuery {
    /// Regex matched against the channel name. Unanchored: it matches anywhere in the
    /// name. Absent matches every channel.
    pub channel_regex: Option<String>,
    /// Regex matched against the `ip:port` of the connection. Unanchored. Absent matches
    /// every connection.
    pub addr_regex: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ChannelSelector {
    addr: Regex,
    channel: Regex,
}

impl ChannelSelector {
    pub fn from_query(q: &StatusQuery) -> Result<Self, StatusApiError> {
        let channel = match q.channel_regex.as_deref() {
            None => Regex::new("").unwrap(),
            Some(x) => Regex::new(x).map_err(|e| StatusApiError::BadChannelRegex(e.to_string()))?,
        };
        let addr = match q.addr_regex.as_deref() {
            None => Regex::new("").unwrap(),
            Some(x) => Regex::new(x).map_err(|e| StatusApiError::BadAddrRegex(e.to_string()))?,
        };
        Ok(Self { addr, channel })
    }

    pub fn addr_pattern(&self) -> &str {
        self.addr.as_str()
    }

    pub fn channel_pattern(&self) -> &str {
        self.channel.as_str()
    }

    pub fn matches_addr(&self, addr: &SocketAddrV4) -> bool {
        self.addr.is_match(&addr.to_string())
    }

    pub fn status_sel(&self, detail: StatusDetail) -> StatusSel {
        StatusSel {
            channel_regex: Some(self.channel.clone()),
            detail,
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct StatusLight {
    /// RFC3339.
    pub ts: String,
    pub ingest_name: String,
    pub conn_count_total: u32,
    pub conn_count_matched: u32,
    pub connset_channel_count_total: u32,
    pub connset_channels: Vec<ChannelStatusLight>,
    pub conns: Vec<StatusLightConn>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct StatusLightConn {
    pub addr: String,
    /// `Connecting`, `Connected` or `Done`.
    pub state: String,
    /// `Init`, `Handshake`, `ActiveCa` or `Done`, once the connection is up.
    pub connected_state: Option<String>,
    /// `Running` or `Done`, once the CA session is active.
    pub activeca_state: Option<String>,
    pub channel_count_total: u32,
    pub channels: Vec<StatusLightChannel>,
    /// Set when this connection did not answer. The connection is still listed, so a
    /// wedged connection is visible rather than silently missing.
    pub error: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct StatusLightChannel {
    pub name: String,
    pub cid: u32,
    pub state: String,
    pub state_elapsed_ms: u64,
    pub proto_inp_buf_len: u32,
    pub outbuf_len: u32,
    pub enum_variants_len: Option<u32>,
    pub event_add_res_cnt: u64,
}

#[derive(Debug, Serialize)]
pub struct StatusFull {
    pub ts: String,
    pub ingest_name: String,
    pub conn_count_total: u32,
    pub conn_count_matched: u32,
    pub connset_channel_count_total: u32,
    pub connset_channels: Vec<StatusFullConnsetChannel>,
    pub conns: Vec<StatusFullConn>,
}

#[derive(Debug, Serialize)]
pub struct StatusFullConnsetChannel {
    pub name: String,
    pub info: ChannelInfo,
}

#[derive(Debug, Serialize)]
pub struct StatusFullConn {
    pub addr: String,
    pub state: String,
    pub connected_state: Option<String>,
    pub activeca_state: Option<String>,
    pub socket_state: Option<serde_json::Value>,
    pub channel_count_total: u32,
    pub channels: Vec<StatusFullChannel>,
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct StatusFullChannel {
    pub name: String,
    pub cid: u32,
    pub state: String,
    pub state_elapsed_ms: u64,
    pub counters: channelheap::channelhandler::Counters,
    /// The `ToSerde` snapshot of the handler and its whole state tree.
    pub handler: Option<Box<channelheap::channelhandler::FullSnap>>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct StatusErrorBody {
    /// Stable machine-readable discriminator: `bad-regex`, `conn2-not-active`,
    /// `connset-unavailable`.
    pub kind: String,
    pub error: String,
}

autoerr::create_error_v1!(
    name(StatusApiError, "StatusApi"),
    enum variants {
        BadChannelRegex(String),
        BadAddrRegex(String),
        Conn2NotActive,
        ConnSet(String),
    },
);

impl StatusApiError {
    pub fn kind(&self) -> &'static str {
        match self {
            StatusApiError::BadChannelRegex(..) | StatusApiError::BadAddrRegex(..) => "bad-regex",
            StatusApiError::Conn2NotActive => "conn2-not-active",
            StatusApiError::ConnSet(..) => "connset-unavailable",
            _ => "error",
        }
    }

    pub fn status_code(&self) -> http::StatusCode {
        use http::StatusCode;
        match self {
            StatusApiError::BadChannelRegex(..) | StatusApiError::BadAddrRegex(..) => StatusCode::BAD_REQUEST,
            StatusApiError::Conn2NotActive => StatusCode::SERVICE_UNAVAILABLE,
            StatusApiError::ConnSet(..) => StatusCode::GATEWAY_TIMEOUT,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl axum::response::IntoResponse for StatusApiError {
    fn into_response(self) -> axum::response::Response {
        let body = StatusErrorBody {
            kind: self.kind().into(),
            error: self.to_string(),
        };
        (self.status_code(), axum::Json(body)).into_response()
    }
}

/// The state names of one connection, as the flat triple the wire types use.
fn state_names(st: &conn::StatusState) -> (String, Option<String>, Option<String>) {
    match st {
        conn::StatusState::Connecting => ("Connecting".into(), None, None),
        conn::StatusState::Done => ("Done".into(), None, None),
        conn::StatusState::Connected(c) => {
            let inner = match &c.state {
                connected::StatusInfoState::Init => ("Init", None),
                connected::StatusInfoState::Handshake => ("Handshake", None),
                connected::StatusInfoState::Done => ("Done", None),
                connected::StatusInfoState::ActiveCa(a) => (
                    "ActiveCa",
                    Some(match &a.state {
                        activeca::StatusInfoState::Running(..) => "Running",
                        activeca::StatusInfoState::Done => "Done",
                    }),
                ),
            };
            ("Connected".into(), Some(inner.0.into()), inner.1.map(Into::into))
        }
    }
}

/// The channel heap of a connection, if it got far enough to have one.
fn chanheap_of(st: &conn::StatusState) -> Option<&channelheap::StatusInfo> {
    match st {
        conn::StatusState::Connected(c) => match &c.state {
            connected::StatusInfoState::ActiveCa(a) => match &a.state {
                activeca::StatusInfoState::Running(_, heap) => Some(heap),
                activeca::StatusInfoState::Done => None,
            },
            _ => None,
        },
        _ => None,
    }
}

pub fn flatten_light(addr: SocketAddrV4, res: Result<conn::StatusInfo, GatherError>) -> StatusLightConn {
    let st = match res {
        Err(e) => {
            return StatusLightConn {
                addr: addr.to_string(),
                state: "Unknown".into(),
                connected_state: None,
                activeca_state: None,
                channel_count_total: 0,
                channels: Vec::new(),
                error: Some(e.to_string()),
            };
        }
        Ok(x) => x,
    };
    let (state, connected_state, activeca_state) = state_names(&st.state);
    let heap = chanheap_of(&st.state);
    let channels = heap.map_or_else(Vec::new, |heap| {
        heap.handlers
            .iter()
            .map(|h| match &h.state {
                channelheap::StatusChannelHandlerState::Active(hi) => StatusLightChannel {
                    name: h.name.clone(),
                    cid: h.cid.to_u32(),
                    state: hi.state_short.clone(),
                    state_elapsed_ms: hi.state_elapsed_ms,
                    proto_inp_buf_len: hi.proto_inp_buf.len as u32,
                    outbuf_len: hi.outbuf.len as u32,
                    enum_variants_len: hi.enum_variants_len,
                    event_add_res_cnt: hi.counters.event_add_res_cnt,
                },
                channelheap::StatusChannelHandlerState::Done => StatusLightChannel {
                    name: h.name.clone(),
                    cid: h.cid.to_u32(),
                    state: "Done".into(),
                    state_elapsed_ms: 0,
                    proto_inp_buf_len: 0,
                    outbuf_len: 0,
                    enum_variants_len: None,
                    event_add_res_cnt: 0,
                },
            })
            .collect()
    });
    StatusLightConn {
        addr: addr.to_string(),
        state,
        connected_state,
        activeca_state,
        channel_count_total: heap.map_or(0, |x| x.channel_count_total),
        channels,
        error: None,
    }
}

pub fn flatten_full(addr: SocketAddrV4, res: Result<conn::StatusInfo, GatherError>) -> StatusFullConn {
    let st = match res {
        Err(e) => {
            return StatusFullConn {
                addr: addr.to_string(),
                state: "Unknown".into(),
                connected_state: None,
                activeca_state: None,
                socket_state: None,
                channel_count_total: 0,
                channels: Vec::new(),
                error: Some(e.to_string()),
            };
        }
        Ok(x) => x,
    };
    let (state, connected_state, activeca_state) = state_names(&st.state);
    let socket_state = match &st.state {
        conn::StatusState::Connected(c) => Some(c.socket_state.clone()),
        _ => None,
    };
    let heap = chanheap_of(&st.state);
    let channel_count_total = heap.map_or(0, |x| x.channel_count_total);
    let channels = match st.state {
        conn::StatusState::Connected(c) => match c.state {
            connected::StatusInfoState::ActiveCa(a) => match a.state {
                activeca::StatusInfoState::Running(_, heap) => heap
                    .handlers
                    .into_iter()
                    .map(|h| {
                        let cid = h.cid.to_u32();
                        match h.state {
                            channelheap::StatusChannelHandlerState::Active(hi) => StatusFullChannel {
                                name: h.name,
                                cid,
                                state: hi.state_short,
                                state_elapsed_ms: hi.state_elapsed_ms,
                                counters: hi.counters,
                                handler: hi.full,
                            },
                            channelheap::StatusChannelHandlerState::Done => StatusFullChannel {
                                name: h.name,
                                cid,
                                state: "Done".into(),
                                state_elapsed_ms: 0,
                                counters: channelheap::channelhandler::Counters::zero(),
                                handler: None,
                            },
                        }
                    })
                    .collect(),
                activeca::StatusInfoState::Done => Vec::new(),
            },
            _ => Vec::new(),
        },
        _ => Vec::new(),
    };
    StatusFullConn {
        addr: addr.to_string(),
        state,
        connected_state,
        activeca_state,
        socket_state,
        channel_count_total,
        channels,
        error: None,
    }
}

pub fn assemble_light(res: StatusV1Res) -> StatusLight {
    let connset_channels = match res.connset_channels {
        ConnsetChannels::Light(x) => x,
        ConnsetChannels::Full(..) => Vec::new(),
    };
    StatusLight {
        ts: res.ts,
        ingest_name: res.ingest_name,
        conn_count_total: res.conn_count_total,
        conn_count_matched: res.conn_count_matched,
        connset_channel_count_total: res.connset_channel_count_total,
        connset_channels,
        conns: res.conns.into_iter().map(|(a, r)| flatten_light(a, r)).collect(),
    }
}

pub fn assemble_full(res: StatusV1Res) -> StatusFull {
    let connset_channels = match res.connset_channels {
        ConnsetChannels::Full(x) => x
            .into_iter()
            .map(|(name, info)| StatusFullConnsetChannel { name, info })
            .collect(),
        ConnsetChannels::Light(..) => Vec::new(),
    };
    StatusFull {
        ts: res.ts,
        ingest_name: res.ingest_name,
        conn_count_total: res.conn_count_total,
        conn_count_matched: res.conn_count_matched,
        connset_channel_count_total: res.connset_channel_count_total,
        connset_channels,
        conns: res.conns.into_iter().map(|(a, r)| flatten_full(a, r)).collect(),
    }
}

async fn conn2_ctrls(ctrls: &Arc<dyn CaIngestCtrls>) -> Result<Box<dyn Conn2Ctrls>, StatusApiError> {
    ctrls.conn2_ctrls().await.ok_or(StatusApiError::Conn2NotActive)
}

/// Cheap overview: one entry per connection and per channel, without the internal
/// state tree. Suitable for frequent polling.
#[utoipa::path(
    get,
    path = "/daqingest/admin/status/light",
    params(StatusQuery),
    responses(
        (status = 200, description = "Status of the matching connections and channels", body = StatusLight),
        (status = 400, description = "A given regex does not compile", body = StatusErrorBody),
        (status = 503, description = "This daemon does not run the v2 ingest path", body = StatusErrorBody),
        (status = 504, description = "The connection set did not answer", body = StatusErrorBody),
    ),
    tag = "daqingest-admin",
)]
pub async fn status_light(
    axum::extract::State(ctrls): axum::extract::State<Arc<dyn CaIngestCtrls>>,
    axum::extract::Query(q): axum::extract::Query<StatusQuery>,
) -> Result<axum::Json<StatusLight>, StatusApiError> {
    let sel = ChannelSelector::from_query(&q)?;
    let c2 = conn2_ctrls(&ctrls).await?;
    let ret = c2
        .status_light_v1(sel)
        .await
        .map_err(|e| StatusApiError::ConnSet(e.to_string()))?;
    Ok(axum::Json(ret))
}

/// The full internal state tree of every matching channel. Expensive; intended for
/// debugging a specific channel or address rather than for polling.
///
/// The response shape follows the internal state machines and is documented as a
/// free-form object on purpose.
#[utoipa::path(
    get,
    path = "/daqingest/admin/status/full",
    params(StatusQuery),
    responses(
        (status = 200, description = "Full status snapshot", body = serde_json::Value),
        (status = 400, description = "A given regex does not compile", body = StatusErrorBody),
        (status = 503, description = "This daemon does not run the v2 ingest path", body = StatusErrorBody),
        (status = 504, description = "The connection set did not answer", body = StatusErrorBody),
    ),
    tag = "daqingest-admin",
)]
pub async fn status_full(
    axum::extract::State(ctrls): axum::extract::State<Arc<dyn CaIngestCtrls>>,
    axum::extract::Query(q): axum::extract::Query<StatusQuery>,
) -> Result<axum::Json<StatusFull>, StatusApiError> {
    let sel = ChannelSelector::from_query(&q)?;
    let c2 = conn2_ctrls(&ctrls).await?;
    let ret = c2
        .status_full_v1(sel)
        .await
        .map_err(|e| StatusApiError::ConnSet(e.to_string()))?;
    Ok(axum::Json(ret))
}

#[cfg(test)]
mod test {
    use super::*;

    fn query(channel: Option<&str>, addr: Option<&str>) -> StatusQuery {
        StatusQuery {
            channel_regex: channel.map(Into::into),
            addr_regex: addr.map(Into::into),
        }
    }

    /// An absent regex must select everything, so a bare GET is a full overview.
    #[test]
    fn selector_absent_matches_all() {
        let sel = ChannelSelector::from_query(&query(None, None)).unwrap();
        assert!(sel.matches_addr(&"10.0.0.5:5064".parse().unwrap()));
        assert!(sel.status_sel(StatusDetail::Light).matches("ANY:CHANNEL"));
    }

    /// The documented semantics are an unanchored substring match; the OpenAPI
    /// description says so and this pins it.
    #[test]
    fn selector_is_unanchored() {
        let sel = ChannelSelector::from_query(&query(Some("BC"), None)).unwrap();
        let ssel = sel.status_sel(StatusDetail::Light);
        assert!(ssel.matches("ABCD"));
        assert!(!ssel.matches("AXD"));
    }

    #[test]
    fn selector_rejects_bad_regex() {
        let e = ChannelSelector::from_query(&query(Some("["), None)).unwrap_err();
        assert_eq!(e.kind(), "bad-regex");
        assert_eq!(e.status_code(), http::StatusCode::BAD_REQUEST);
        let e = ChannelSelector::from_query(&query(None, Some("("))).unwrap_err();
        assert_eq!(e.kind(), "bad-regex");
    }

    /// A connection which did not answer must still appear, with the reason, rather
    /// than dropping out of the document and looking healthy by absence.
    #[test]
    fn flatten_light_keeps_failed_conn() {
        let addr: SocketAddrV4 = "10.0.0.5:5064".parse().unwrap();
        let c = flatten_light(addr, Err(GatherError::Timeout));
        assert_eq!(c.addr, "10.0.0.5:5064");
        assert_eq!(c.error.as_deref(), Some("timeout"));
        assert!(c.channels.is_empty());
        let c = flatten_full(addr, Err(GatherError::Comm("closed".into())));
        assert_eq!(c.error.as_deref(), Some("comm: closed"));
    }

    /// Channels only exist under `Connected/ActiveCa/Running`; every shallower state
    /// must report an empty list rather than fabricate one.
    #[test]
    fn flatten_light_states_without_channels() {
        let addr: SocketAddrV4 = "10.0.0.5:5064".parse().unwrap();
        let mk = |state| conn::StatusInfo {
            ts: time::UtcDateTime::now(),
            addr,
            state,
        };
        let c = flatten_light(addr, Ok(mk(conn::StatusState::Connecting)));
        assert_eq!(c.state, "Connecting");
        assert_eq!(c.connected_state, None);
        assert!(c.channels.is_empty());

        let c = flatten_light(addr, Ok(mk(conn::StatusState::Done)));
        assert_eq!(c.state, "Done");

        let c = flatten_light(
            addr,
            Ok(mk(conn::StatusState::Connected(connected::StatusInfo {
                state: connected::StatusInfoState::Handshake,
                socket_state: serde_json::Value::Null,
            }))),
        );
        assert_eq!(c.state, "Connected");
        assert_eq!(c.connected_state.as_deref(), Some("Handshake"));
        assert_eq!(c.activeca_state, None);
        assert!(c.channels.is_empty());

        let c = flatten_light(
            addr,
            Ok(mk(conn::StatusState::Connected(connected::StatusInfo {
                state: connected::StatusInfoState::ActiveCa(activeca::StatusInfo {
                    state: activeca::StatusInfoState::Done,
                }),
                socket_state: serde_json::Value::Null,
            }))),
        );
        assert_eq!(c.connected_state.as_deref(), Some("ActiveCa"));
        assert_eq!(c.activeca_state.as_deref(), Some("Done"));
        assert!(c.channels.is_empty());
    }
}
