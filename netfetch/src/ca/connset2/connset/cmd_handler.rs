use super::ConnSet;
use super::Error;
use crate::ca::conn2::asynchan;
use netpod::futdbg::FutDbg;
use netpod::futdbg::FutDbgBox;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::task::Context;

macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }

fn handle_test01(
    self1: Pin<&mut ConnSet>,
    cmd: String,
    mut tx: asynchan::Sender<serde_json::Value>,
    _cx: &mut Context,
) -> Option<FutDbg<Result<(), Error>>> {
    let selfname = "handle_test01";
    // This is just for testing
    warn!("{selfname}");
    info!("{selfname}");
    debug!("{selfname}");
    trace!("{selfname}");
    #[derive(Debug, Deserialize)]
    struct DynCmdV03WithConnAddr {
        conn_addr_regex: String,
    }
    if let Ok(cmd3) = serde_json::from_str::<DynCmdV03WithConnAddr>(&cmd) {
        info!("{cmd3:?}");
        match serde_json::from_str::<serde_json::Value>(&cmd) {
            Ok(cmd) => {
                use serde_json::json;
                info!("{cmd:?}");

                // TODO check first if we should scatter the command over connections or if it is simply for us.

                // TODO scatter gather

                match regex::Regex::new(&cmd3.conn_addr_regex) {
                    Ok(re) => {
                        let comms = self1
                            .ca_conns
                            .iter()
                            .filter_map(|x| {
                                let s1 = x.0.to_string();
                                if re.is_match(&s1) {
                                    Some((x.0.clone(), x.1.comm.clone()))
                                } else {
                                    None
                                }
                            })
                            .collect::<Vec<_>>();
                        let fut = async move {
                            let mut ret = BTreeMap::new();
                            for (addr, mut comm) in comms {
                                // TODO maybe use a recursive command format and extract sub-part?
                                let val = comm.dyn_cmd_v03(cmd.clone()).await;
                                ret.insert(addr.to_string(), val);
                            }
                            let x = json!({
                                "conns": ret,
                            });
                            let _ = tx.send(x).await;
                            Ok(())
                        };
                        Some(fut.box2())
                    }
                    Err(e) => {
                        let val = json!({
                            "error": e.to_string(),
                        });
                        let _ = tx.try_send(val);
                        None
                    }
                }
            }
            Err(e) => {
                let val = serde_json::json!({
                    "type": "error",
                    "msg": format!("{e}"),
                });
                let _ = tx.try_send(val);
                None
            }
        }
    } else {
        let val = serde_json::json!({
            "type": "error",
            "msg": format!("not a DynCmdV03WithConnAddr"),
        });
        let _ = tx.try_send(val);
        None
    }
}

fn connset_state_channel_all(
    self1: Pin<&mut ConnSet>,
    cmd: String,
    mut tx: asynchan::Sender<serde_json::Value>,
    _cx: &mut Context,
) -> Option<FutDbg<Result<(), Error>>> {
    use serde_json::json;
    #[derive(Debug, Deserialize)]
    struct Cmd {
        tmp: Option<String>,
    }
    if let Ok(cmd2) = serde_json::from_str::<Cmd>(&cmd) {
        let _ = &cmd2.tmp;
        info!("{cmd2:?}");
        let x = json!({
            "channels": self1.channels.iter().map(|(chn, cc)| (chn, cc.channel.channel_info())).collect::<Vec<_>>(),
        });
        let _ = tx.try_send(x);
        None
    } else {
        let val = serde_json::json!({
            "type": "error",
            "msg": format!("command bad"),
        });
        let _ = tx.try_send(val);
        None
    }
}

impl ConnSet {
    pub(super) fn handle_dyn_cmd_v03(
        self: Pin<&mut Self>,
        cmd: String,
        mut tx: asynchan::Sender<serde_json::Value>,
        cx: &mut Context,
    ) -> Option<FutDbg<Result<(), Error>>> {
        #[derive(Debug, Deserialize)]
        struct DynCmdV03Base {
            connset_cmd: String,
        }
        if let Ok(cmd2) = serde_json::from_str::<DynCmdV03Base>(&cmd) {
            info!("{cmd2:?}");
            if cmd2.connset_cmd == "test01" {
                handle_test01(self, cmd, tx, cx)
            } else if cmd2.connset_cmd == "connset_state_channel_all" {
                connset_state_channel_all(self, cmd, tx, cx)
            } else {
                let val = serde_json::json!({
                    "type": "error",
                    "msg": format!("command unknown"),
                });
                let _ = tx.try_send(val);
                None
            }
        } else {
            let val = serde_json::json!({
                "type": "error",
                "msg": format!("not a DynCmdV03Base"),
            });
            let _ = tx.try_send(val);
            None
        }
    }
}
