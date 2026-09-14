use crate::conf::ChannelConfig;
use crate::metrics::CaIngestCtrls;
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
