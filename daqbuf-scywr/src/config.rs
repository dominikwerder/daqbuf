use crate::iteminsertqueue::InsertTarget;
use netpod::ttl::RetentionTime;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct ScyllaIngestConfig {
    keyspace: String,
    hosts: Vec<String>,
    rt: RetentionTime,
}

impl ScyllaIngestConfig {
    pub fn new<I, H, K1>(hosts: I, ks: K1, rt: RetentionTime) -> Self
    where
        I: IntoIterator<Item = H>,
        H: Into<String>,
        K1: Into<String>,
    {
        Self {
            keyspace: ks.into(),
            hosts: hosts.into_iter().map(Into::into).collect(),
            rt,
        }
    }

    pub fn keyspace(&self) -> &String {
        &self.keyspace
    }

    pub fn hosts(&self) -> &Vec<String> {
        &self.hosts
    }

    pub fn short_name(&self) -> String {
        format!(
            "ScyllaIngestConfig {{ {:?}, {:?}, {:?} }}",
            self.hosts.get(0),
            self.keyspace,
            self.rt
        )
    }
}

#[derive(Debug, Clone)]
pub struct ScyllaInsertSetConfig {
    st_rf1: ScyllaIngestConfig,
    st_rf3: ScyllaIngestConfig,
    mt_rf3: ScyllaIngestConfig,
    lt_rf3: ScyllaIngestConfig,
}

impl ScyllaInsertSetConfig {
    pub fn new(
        st_rf1: ScyllaIngestConfig,
        st_rf3: ScyllaIngestConfig,
        mt_rf3: ScyllaIngestConfig,
        lt_rf3: ScyllaIngestConfig,
    ) -> Self {
        Self {
            st_rf1,
            st_rf3,
            mt_rf3,
            lt_rf3,
        }
    }

    pub fn for_target(&self, target: InsertTarget) -> &ScyllaIngestConfig {
        match target {
            InsertTarget::StRf1 => &self.st_rf1,
            InsertTarget::StRf3 => &self.st_rf3,
            InsertTarget::MtRf3 => &self.mt_rf3,
            InsertTarget::LtRf3 => &self.lt_rf3,
        }
    }
}
