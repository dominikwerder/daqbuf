use netpod::ScyllaConfig;
use netpod::ScyllaConfigMultiKeyspace;
use netpod::log;
use scylla::client::execution_profile::ExecutionProfileBuilder;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::NewSessionError;
use scylla::statement::Consistency;
use std::num::NonZero;
use std::sync::Arc;

autoerr::create_error_v1!(
    name(Error, "ScyllaSessionCreate"),
    enum variants {
        ScyllaSessionNew(#[from] NewSessionError),
        ScyllaUseKeyspace(#[from] scylla::errors::UseKeyspaceError),
    },
);

pub async fn create_scy_session(scyconf: &ScyllaConfig) -> Result<Arc<Session>, Error> {
    let scy = create_scy_session_no_ks(scyconf).await?;
    scy.use_keyspace(&scyconf.keyspace, true).await?;
    let ret = Arc::new(scy);
    Ok(ret)
}

trait ScyllaHostSet {
    fn hosts(&self) -> &Vec<String>;
}

impl ScyllaHostSet for &ScyllaConfig {
    fn hosts(&self) -> &Vec<String> {
        &self.hosts
    }
}

impl ScyllaHostSet for &ScyllaConfigMultiKeyspace {
    fn hosts(&self) -> &Vec<String> {
        &self.hosts
    }
}

pub async fn create_scy_session_no_ks<H>(hosts: H) -> Result<Session, Error>
where
    H: ScyllaHostSet,
{
    log::info!("creating scylla connection");
    let scy = SessionBuilder::new()
        .pool_size(scylla::client::PoolSize::PerHost(NonZero::new(4).unwrap()))
        .known_nodes(hosts.hosts())
        .default_execution_profile_handle(
            ExecutionProfileBuilder::default()
                .consistency(Consistency::Two)
                .build()
                .into_handle(),
        )
        .build()
        .await?;
    Ok(scy)
}
