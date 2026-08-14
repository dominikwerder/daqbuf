use super::ConnSet;
use super::Error;
use crate::asynchan;
use futures::FutureExt;
use futures::StreamExt;
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

fn connset_state_channel_full(
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
        let chsas = self1
            .ca_conns
            .iter()
            .map(|(&addr, connreg)| (addr, connreg.shutting_down, connreg.comm.clone()))
            .collect::<Vec<_>>();
        let chs = self1
            .channels
            .iter()
            .map(|(chn, cc)| (chn.clone(), (cc.channel.channel_info())))
            .collect::<BTreeMap<_, _>>();
        let fut = async move {
            let mut conns = Vec::new();
            for (addr, shtdwn, mut comm) in chsas {
                let cmd = json!({
                   "caconn_cmd": "conn_state_channel_full",
                });
                let res = comm.dyn_cmd_v03(cmd).await;
                conns.push((addr, shtdwn, res));
            }
            let x = json!({
                "chs": chs,
                "conns": conns,
            });
            let _ = tx.try_send(x);
            Ok(())
        };
        Some(fut.box2())
    } else {
        let val = serde_json::json!({
            "type": "error",
            "msg": format!("command bad"),
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

fn ca_conn_state_proto(
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
        let comms = self1
            .ca_conns
            .iter()
            .map(|(addr, reg)| (*addr, reg.comm.clone()))
            .collect::<Vec<_>>();
        let fut = async move {
            let sss = futures::stream::iter(comms)
                .map(|(addr, mut comm)| {
                    let cmd4 = json!({
                        "caconn_cmd": "ca_conn_state_proto",
                    });
                    let cmd4 = serde_json::to_value(cmd4).unwrap();
                    async move { comm.dyn_cmd_v03(cmd4).map(|x| (addr, x)).await }
                })
                .buffer_unordered(16)
                .collect::<Vec<_>>()
                .await;
            let x = sss.into_iter().collect::<BTreeMap<_, _>>();
            let x = json!({
                "connections": x,
            });
            let _ = tx.try_send(x);
            Ok(())
        };
        Some(fut.box2())
    } else {
        let val = serde_json::json!({
            "type": "error",
            "msg": format!("command bad"),
        });
        let _ = tx.try_send(val);
        None
    }
}

fn channel_details_v00(
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
        let comms = self1
            .ca_conns
            .iter()
            .map(|(addr, reg)| (*addr, reg.comm.clone()))
            .collect::<Vec<_>>();
        let fut = async move {
            let sss = futures::stream::iter(comms)
                .map(|(addr, mut comm)| {
                    let cmd4: serde_json::Value = serde_json::from_str(&cmd).unwrap();
                    async move { comm.dyn_cmd_v03(cmd4).map(|x| (addr, x)).await }
                })
                .buffer_unordered(16)
                .collect::<Vec<_>>()
                .await;
            let x = sss.into_iter().collect::<BTreeMap<_, _>>();
            let x = json!({
                "results": x,
            });
            let _ = tx.try_send(x);
            Ok(())
        };
        Some(fut.box2())
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
            } else if cmd2.connset_cmd == "connset_state_channel_full" {
                connset_state_channel_full(self, cmd, tx, cx)
            } else if cmd2.connset_cmd == "connset_state_channel_all" {
                connset_state_channel_all(self, cmd, tx, cx)
            } else if cmd2.connset_cmd == "ca_conn_state_proto" {
                ca_conn_state_proto(self, cmd, tx, cx)
            } else if cmd2.connset_cmd == "channel_details_v00" {
                channel_details_v00(self, cmd, tx, cx)
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
