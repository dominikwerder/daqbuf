use super::ChannelAdd;
use super::ConnSetCmd;
use super::ConnSetCmdKind;
use crate::ca::conn2::asynchan;

autoerr::create_error_v1!(
    name(Error, "ConnSetCmder"),
    enum variants {
        Send,
        Recv(#[from] asynchan::RecvError),
        ConnSet(#[from] super::Error),
    },
);

impl<T> From<asynchan::SendError<T>> for Error {
    fn from(_value: asynchan::SendError<T>) -> Self {
        Error::Send
    }
}

#[derive(Debug)]
pub struct ConnSetCmder {
    tx: asynchan::Sender<ConnSetCmd>,
}

impl ConnSetCmder {
    pub fn new(cmd_tx: asynchan::Sender<ConnSetCmd>) -> Self {
        Self { tx: cmd_tx }
    }

    pub async fn channel_add(&self, ch_cfg: crate::conf::ChannelConfig) -> Result<(), Error> {
        let (restx, mut rx) = asynchan::bounded(1, "ConnSetCmder-channel_add-resp");
        let add = ChannelAdd { ch_cfg, restx };
        let cmd = ConnSetCmd {
            kind: ConnSetCmdKind::ChannelAdd(add),
        };
        self.tx.clone().send(cmd).await?;
        let res = rx.recv().await??;
        Ok(res)
    }
}
