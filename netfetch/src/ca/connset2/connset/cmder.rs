use super::ChannelAdd;
use super::ConnSetCmd;
use super::ConnSetCmdKind;
use crate::ca::conn2::asynchan;

autoerr::create_error_v1!(
    name(Error, "ConnSetCmder"),
    enum variants {
        Logic,
    },
);

impl<T> From<asynchan::SendError<T>>

#[derive(Debug)]
pub struct ConnSetCmder {
    tx: asynchan::Sender<ConnSetCmd>,
}

impl ConnSetCmder {
    pub fn new(cmd_tx: asynchan::Sender<ConnSetCmd>) -> Self {
        Self { tx: cmd_tx }
    }

    pub async fn channel_add(&self, ch_cfg: crate::conf::ChannelConfig) -> Result<(), Error> {
        let (restx, rx) = async_channel::bounded(1);
        let add = ChannelAdd { ch_cfg, restx };
        let cmd = ConnSetCmd {
            kind: ConnSetCmdKind::ChannelAdd(add),
        };
        self.tx.send(cmd).await?;
        let res = rx.recv().await?;
        res
    }
}
