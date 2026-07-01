use crate::bodystream::response;
use crate::err::Error;
use crate::ReqCtx;
use http::StatusCode;
use httpclient::IntoBody;
use httpclient::Requ;
use httpclient::StreamResponse;
use httpclient::ToJsonBody;
use netpod::log::*;
use netpod::NodeConfigCached;
use netpod::NodeStatus;
use netpod::NodeStatusArchiverAppliance;
use netpod::ServiceVersion;
use netpod::TableSizes;
use std::collections::VecDeque;
use std::time::Duration;
use taskrun::tokio;

#[allow(unused)]
async fn table_sizes(node_config: &NodeConfigCached) -> Result<TableSizes, Error> {
    let ret = dbconn::table_sizes(node_config).await?;
    Ok(ret)
}

pub struct StatusNodesRecursive {}

impl StatusNodesRecursive {
    pub fn path() -> &'static str {
        "/api/4/private/status/nodes/recursive"
    }

    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == Self::path() {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(
        &self,
        req: Requ,
        ctx: &ReqCtx,
        node_config: &NodeConfigCached,
        service_version: &ServiceVersion,
    ) -> Result<StreamResponse, Error> {
        let res = tokio::time::timeout(
            Duration::from_millis(1200),
            self.status(req, ctx, node_config, service_version),
        )
        .await;
        let res = match res {
            Ok(res) => res,
            Err(e) => {
                let e = Error::from(e).add_public_msg("see timeout");
                return Ok(crate::bodystream::ToPublicResponse::to_public_response(&e));
            }
        };
        match res {
            Ok(status) => {
                let ret = response(StatusCode::OK).body(ToJsonBody::from(&status).into_body())?;
                Ok(ret)
            }
            Err(e) => {
                error!("StatusNodesRecursive sees: {e}");
                let ret = crate::bodystream::ToPublicResponse::to_public_response(&e);
                Ok(ret)
            }
        }
    }

    async fn status(
        &self,
        req: Requ,
        _ctx: &ReqCtx,
        node_config: &NodeConfigCached,
        service_version: &ServiceVersion,
    ) -> Result<NodeStatus, Error> {
        let (_head, _body) = req.into_parts();
        let archiver_appliance_status = match node_config.node.archiver_appliance.as_ref() {
            Some(k) => {
                let mut st = Vec::new();
                for p in &k.data_base_paths {
                    let _m = match tokio::fs::metadata(p).await {
                        Ok(m) => m,
                        Err(_e) => {
                            st.push((p.into(), false));
                            continue;
                        }
                    };
                    let _ = match tokio::fs::read_dir(p).await {
                        Ok(rd) => rd,
                        Err(_e) => {
                            st.push((p.into(), false));
                            continue;
                        }
                    };
                    st.push((p.into(), true));
                }
                Some(NodeStatusArchiverAppliance { readable: st })
            }
            None => None,
        };
        let database_size = dbconn::database_size(node_config).await.map_err(|e| format!("{e}"));
        let ret = NodeStatus {
            name: format!("{}:{}", node_config.node.host, node_config.node.port),
            version: service_version.to_string(),
            is_sf_databuffer: node_config.node.sf_databuffer.is_some(),
            is_archiver_engine: node_config.node.channel_archiver.is_some(),
            is_archiver_appliance: node_config.node.archiver_appliance.is_some(),
            database_size: Some(database_size),
            //table_sizes: Some(table_sizes(node_config).await.map_err(Into::into)),
            archiver_appliance_status,
            subs: VecDeque::new(),
        };
        Ok(ret)
    }
}
