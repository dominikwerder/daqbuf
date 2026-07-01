pub mod client;
pub mod err;
pub mod nodes;
#[cfg(test)]
pub mod test;

use daqbuf_err::Error;
use futures_util::TryFutureExt;
use netpod::Cluster;
use netpod::NodeConfig;
use netpod::NodeConfigCached;
use netpod::ProxyConfig;
use netpod::ServiceVersion;
use tokio::task::JoinHandle;

pub fn spawn_test_hosts(cluster: Cluster) -> Vec<JoinHandle<Result<(), Error>>> {
    let service_version = ServiceVersion {
        major: 0,
        minor: 0,
        patch: 0,
        pre: None,
    };
    let mut ret = Vec::new();
    for node in &cluster.nodes {
        let node_config = NodeConfig {
            cluster: cluster.clone(),
            name: format!("{}:{}", node.host, node.port),
        };
        let node_config: Result<NodeConfigCached, netpod::Error> = node_config.into();
        let node_config = node_config.unwrap();
        let h = tokio::spawn(httpret::host(node_config, service_version.clone()).map_err(Error::from));
        ret.push(h);
    }

    // TODO spawn also two proxy nodes

    ret
}

pub async fn run_node(node_config: NodeConfigCached, service_version: ServiceVersion) -> Result<(), Error> {
    httpret::host(node_config, service_version).await?;
    Ok(())
}

pub async fn run_proxy(proxy_config: ProxyConfig, service_version: ServiceVersion) -> Result<(), Error> {
    httpret::proxy::proxy(proxy_config, service_version).await?;
    Ok(())
}

pub async fn test_log() {
    use netpod::log::*;
    error!("------");
    warn!("------");
    info!("------");
    debug!("------");
    trace!("------");
    httpret::test_log().await;
}
