use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MspEv(u64);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LspEv(u64);
