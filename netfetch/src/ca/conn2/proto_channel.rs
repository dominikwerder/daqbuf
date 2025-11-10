use async_channel::Sender;

#[derive(Debug, Clone)]
pub struct ProtoOutChannel {
    tx: Sender<ca_proto::ca::proto::CaMsg>,
}

impl ProtoOutChannel {
    pub fn tx(&self) -> &Sender<ca_proto::ca::proto::CaMsg> {
        &self.tx
    }
}
