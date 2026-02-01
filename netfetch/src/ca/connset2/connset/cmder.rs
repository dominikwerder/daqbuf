use super::ChannelAdd;
use super::ChannelRemove;
use super::ConnSetCmd;
use super::ConnSetCmdKind;
use crate::ca::conn2::asynchan;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace2 { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace3 { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace4 { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace_pending { ($($arg:tt)*) => { if true { log::info!("{}  Pending", format_args!($($arg)*)); } }; }

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

#[derive(Debug, Clone)]
pub struct ConnSetCmder {
    tx: asynchan::Sender<ConnSetCmd>,
}

impl ConnSetCmder {
    pub fn new(cmd_tx: asynchan::Sender<ConnSetCmd>) -> Self {
        Self { tx: cmd_tx }
    }

    pub async fn channel_add(&self, ch_cfg: crate::conf::ChannelConfig) -> Result<(), Error> {
        let selfname = "channel_add";
        let mut tx = self.tx.clone();
        let (done_tx, mut done_rx) = asynchan::bounded(1, "ConnSetCmder-channel_add-resp");
        let cmd = ConnSetCmd {
            kind: ConnSetCmdKind::ChannelAdd(ChannelAdd { ch_cfg, done_tx }),
        };
        trace2!("{selfname} tx.send");
        let _ = tx.send(cmd).await?;
        trace2!("{selfname} done_rx.recv");
        done_rx.recv().await??;
        trace2!("{selfname} done_rx.recv done");
        Ok(())
    }

    /// When this async fn completes, the channel has been removed.
    pub async fn channel_remove<S: Into<String>>(&self, name: S) -> Result<(), Error> {
        let selfname = "channel_remove";
        let mut tx = self.tx.clone();
        let (done_tx, mut done_rx) = asynchan::bounded(1, "ConnSetCmder-channel_remove-resp");
        let cmd = ConnSetCmd {
            kind: ConnSetCmdKind::ChannelRemove(ChannelRemove {
                name: name.into(),
                done_tx,
            }),
        };
        trace2!("{selfname} tx.send");
        let _ = tx.send(cmd).await?;
        trace2!("{selfname} done_rx.recv");
        let res = done_rx.recv().await??;
        trace2!("{selfname} done");
        Ok(res)
    }

    pub async fn shutdown(&self) -> Result<(), Error> {
        let mut tx = self.tx.clone();
        let cmd = ConnSetCmd {
            kind: ConnSetCmdKind::Shutdown,
        };
        let _ = tx.send(cmd).await?;
        Ok(())
    }
}
