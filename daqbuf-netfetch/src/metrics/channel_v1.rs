use crate::conf::ChannelConfig;
use crate::metrics::CaIngestCtrls;
use crate::metrics::Conn2Ctrls;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
use utoipa::IntoParams;
use utoipa::ToSchema;

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ChannelAddQuery {
    pub name: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ChannelAddErrorBody {
    /// Stable machine-readable discriminator: `add-failed`.
    pub kind: String,
    pub error: String,
}

autoerr::create_error_v1!(
    name(ChannelAddApiError, "ChannelAddApi"),
    enum variants {
        AddFailed(String),
    },
);

impl ChannelAddApiError {
    pub fn kind(&self) -> &'static str {
        match self {
            ChannelAddApiError::AddFailed(..) => "add-failed",
            _ => "error",
        }
    }

    pub fn status_code(&self) -> axum::http::StatusCode {
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    }
}

impl axum::response::IntoResponse for ChannelAddApiError {
    fn into_response(self) -> axum::response::Response {
        let body = ChannelAddErrorBody {
            kind: self.kind().into(),
            error: self.to_string(),
        };
        (self.status_code(), axum::Json(body)).into_response()
    }
}

#[utoipa::path(
    get,
    path = "/add",
    params(ChannelAddQuery),
    responses(
        (status = 200, description = "Whether the channel was added", body = bool),
        (status = 500, description = "The channel could not be added", body = ChannelAddErrorBody),
    ),
    tag = "daqingest-channel",
)]
pub async fn channel_add(
    axum::extract::State(ctrls): axum::extract::State<Arc<dyn CaIngestCtrls>>,
    axum::extract::Query(q): axum::extract::Query<ChannelAddQuery>,
) -> Result<axum::Json<bool>, ChannelAddApiError> {
    let conf = ChannelConfig::st_monitor(&q.name, "api");
    ctrls
        .channel_add(conf)
        .await
        .map_err(|e| ChannelAddApiError::AddFailed(e.to_string()))?;
    Ok(axum::Json(true))
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ChannelRemoveQuery {
    /// `ip:port` of the connection.
    pub addr: String,
    pub backend: String,
    pub name: String,
}

#[utoipa::path(
    get,
    path = "/remove",
    params(ChannelRemoveQuery),
    responses(
        (status = 200, description = "Whether the channel was removed", body = bool),
    ),
    tag = "daqingest-channel",
)]
pub async fn channel_remove(axum::extract::Query(_q): axum::extract::Query<ChannelRemoveQuery>) -> axum::Json<bool> {
    log::error!("TODO channel_remove");
    axum::Json(false)
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ChannelReadNotifyQuery {
    pub name: String,
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct ChannelReadNotifyResult {
    pub ok: bool,
    /// The read value. Free-form since its shape depends on the channel's native DBR type.
    #[schema(value_type = Object)]
    pub value: Option<serde_json::Value>,
    /// Set when `ok` is false, e.g. `chname not found`, `timeout`, or a state mismatch.
    pub error: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ChannelReadNotifyErrorBody {
    /// Stable machine-readable discriminator: `conn2-not-active`, `backend-error`.
    pub kind: String,
    pub error: String,
}

autoerr::create_error_v1!(
    name(ChannelReadNotifyApiError, "ChannelReadNotifyApi"),
    enum variants {
        Conn2NotActive,
        Backend(String),
    },
);

impl ChannelReadNotifyApiError {
    pub fn kind(&self) -> &'static str {
        match self {
            ChannelReadNotifyApiError::Conn2NotActive => "conn2-not-active",
            ChannelReadNotifyApiError::Backend(..) => "backend-error",
            _ => "error",
        }
    }

    pub fn status_code(&self) -> axum::http::StatusCode {
        match self {
            ChannelReadNotifyApiError::Conn2NotActive => axum::http::StatusCode::SERVICE_UNAVAILABLE,
            _ => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl axum::response::IntoResponse for ChannelReadNotifyApiError {
    fn into_response(self) -> axum::response::Response {
        let body = ChannelReadNotifyErrorBody {
            kind: self.kind().into(),
            error: self.to_string(),
        };
        (self.status_code(), axum::Json(body)).into_response()
    }
}

/// Issue an on-demand `ReadNotify` for the channel and return its current value. Fails
/// with an `ok: false` result (not an HTTP error) when the channel name is unknown, the
/// channel is not yet in its `Running` state, or too many ad-hoc reads are already
/// in-flight for it.
#[utoipa::path(
    get,
    path = "/read_notify",
    params(ChannelReadNotifyQuery),
    responses(
        (status = 200, description = "Result of the on-demand ReadNotify", body = ChannelReadNotifyResult),
        (status = 500, description = "The command could not be executed", body = ChannelReadNotifyErrorBody),
        (status = 503, description = "This daemon does not run the v2 ingest path", body = ChannelReadNotifyErrorBody),
    ),
    tag = "daqingest-channel",
)]
pub async fn channel_read_notify(
    axum::extract::State(ctrls): axum::extract::State<Arc<dyn CaIngestCtrls>>,
    axum::extract::Query(q): axum::extract::Query<ChannelReadNotifyQuery>,
) -> Result<axum::Json<ChannelReadNotifyResult>, ChannelReadNotifyApiError> {
    let c2 = ctrls
        .conn2_ctrls()
        .await
        .ok_or(ChannelReadNotifyApiError::Conn2NotActive)?;
    let cmd = serde_json::json!({
        "type": "dyn_cmd_v03",
        "connset_cmd": "channel_read_notify_v01",
        "caconn_cmd": "channel_read_notify_v01",
        "chname": q.name,
    });
    let v = c2
        .cmd_dyn_v1(cmd.to_string())
        .await
        .map_err(|e| ChannelReadNotifyApiError::Backend(e.to_string()))?;
    let res =
        serde_json::from_value::<ChannelReadNotifyResult>(v.clone()).unwrap_or_else(|_| ChannelReadNotifyResult {
            ok: false,
            value: None,
            error: Some(
                v.get("error")
                    .and_then(|e| e.as_str())
                    .map(str::to_string)
                    .unwrap_or_else(|| v.to_string()),
            ),
        });
    Ok(axum::Json(res))
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ChannelTraceQuery {
    pub name: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ChannelTraceResult {
    /// The stashed protocol/channel trace events for this channel, most-recent-last.
    /// `null` if nothing has been traced for this channel name yet.
    #[schema(value_type = Object)]
    pub trace: serde_json::Value,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ChannelTraceErrorBody {
    /// Stable machine-readable discriminator: `conn2-not-active`, `backend-error`.
    pub kind: String,
    pub error: String,
}

autoerr::create_error_v1!(
    name(ChannelTraceApiError, "ChannelTraceApi"),
    enum variants {
        Conn2NotActive,
        Backend(String),
    },
);

impl ChannelTraceApiError {
    pub fn kind(&self) -> &'static str {
        match self {
            ChannelTraceApiError::Conn2NotActive => "conn2-not-active",
            ChannelTraceApiError::Backend(..) => "backend-error",
            _ => "error",
        }
    }

    pub fn status_code(&self) -> axum::http::StatusCode {
        match self {
            ChannelTraceApiError::Conn2NotActive => axum::http::StatusCode::SERVICE_UNAVAILABLE,
            _ => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl axum::response::IntoResponse for ChannelTraceApiError {
    fn into_response(self) -> axum::response::Response {
        let body = ChannelTraceErrorBody {
            kind: self.kind().into(),
            error: self.to_string(),
        };
        (self.status_code(), axum::Json(body)).into_response()
    }
}

/// The stashed trace of recent protocol/channel events (`Created`, `ReadNotify`,
/// `ReadNotifyRes`, ...) for one channel name, kept by `ConnSet`'s `ChannelTraceStash`.
#[utoipa::path(
    get,
    path = "/trace",
    params(ChannelTraceQuery),
    responses(
        (status = 200, description = "Traced events for the channel", body = ChannelTraceResult),
        (status = 500, description = "The command could not be executed", body = ChannelTraceErrorBody),
        (status = 503, description = "This daemon does not run the v2 ingest path", body = ChannelTraceErrorBody),
    ),
    tag = "daqingest-channel",
)]
pub async fn channel_trace(
    axum::extract::State(ctrls): axum::extract::State<Arc<dyn CaIngestCtrls>>,
    axum::extract::Query(q): axum::extract::Query<ChannelTraceQuery>,
) -> Result<axum::Json<ChannelTraceResult>, ChannelTraceApiError> {
    let c2 = ctrls.conn2_ctrls().await.ok_or(ChannelTraceApiError::Conn2NotActive)?;
    let cmd = serde_json::json!({
        "type": "dyn_cmd_v03",
        "connset_cmd": "channel_trace_v01",
        "chname": q.name,
    });
    let v = c2
        .cmd_dyn_v1(cmd.to_string())
        .await
        .map_err(|e| ChannelTraceApiError::Backend(e.to_string()))?;
    Ok(axum::Json(ChannelTraceResult { trace: v }))
}
