use ca_proto_tokio::tcpasyncwriteread::TcpAsyncWriteRead;
use clap::Parser;
use daqbuf_ca_proto::pva::Error;
use daqbuf_ca_proto::pva::proto::ChannelReq;
use daqbuf_ca_proto::pva::proto::ChannelRequestInit;
use daqbuf_ca_proto::pva::proto::ChannelRequestOp;
use daqbuf_ca_proto::pva::proto::ConnValidRes;
use daqbuf_ca_proto::pva::proto::CreateChannelReq;
use daqbuf_ca_proto::pva::proto::PvaItem;
use daqbuf_ca_proto::pva::proto::PvaMsgTy;
use daqbuf_ca_proto::pva::proto::PvaProto;
use daqbuf_ca_proto::pva::proto::PvaRequest;
use daqbuf_ca_proto::pva::pvdata::StatusKind;
use futures_util::StreamExt;
use std::process::ExitCode;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;

#[derive(Parser)]
#[command(about = "Connect to a PV via EPICS PV Access, read its value once, print it as JSON, and exit")]
struct Opts {
    #[arg(long)]
    host: String,
    #[arg(long, default_value_t = 5075)]
    port: u16,
    #[arg(long)]
    pv: String,
    #[arg(long)]
    request: Option<String>,
    #[arg(long, default_value_t = 5)]
    timeout_secs: u64,
}

const CLIENT_CID: u32 = 1;
const REQUEST_ID: u32 = 1;

async fn run(opts: &Opts) -> Result<serde_json::Value, Error> {
    let tcp = TcpStream::connect((opts.host.as_str(), opts.port)).await?;
    tcp.set_nodelay(true).ok();
    let remote_name = format!("{}:{}", opts.host, opts.port);
    let tcp = TcpAsyncWriteRead::from(tcp);
    let mut proto = PvaProto::new(tcp, None, remote_name, 1024 * 1024);

    let pv_request = match &opts.request {
        Some(s) => PvaRequest::parse(s)?,
        None => PvaRequest::all(),
    };

    let mut server_cid = None;
    loop {
        let item = match proto.next().await {
            Some(x) => x?,
            None => return Err(Error::NeitherPendingNorProgress),
        };
        let msg = match item {
            PvaItem::Empty => continue,
            PvaItem::Msg(m) => m,
        };
        match msg.ty {
            PvaMsgTy::ConnectionValidationReq(_) => {
                proto.push_out(PvaMsgTy::ConnectionValidationRes(ConnValidRes::anonymous()));
            }
            PvaMsgTy::ConnectionValidated(status) => {
                if status.kind != StatusKind::Ok && status.kind != StatusKind::Warning {
                    return Err(Error::LogicError);
                }
                proto.push_out(PvaMsgTy::CreateChannelReq(CreateChannelReq {
                    channels: vec![ChannelReq {
                        client_cid: CLIENT_CID,
                        name: opts.pv.clone(),
                    }],
                }));
            }
            PvaMsgTy::CreateChannelRes(res) => {
                if res.client_cid != CLIENT_CID {
                    continue;
                }
                if res.status.kind != StatusKind::Ok && res.status.kind != StatusKind::Warning {
                    eprintln!("create channel failed: {}", res.status.message);
                    return Err(Error::LogicError);
                }
                server_cid = Some(res.server_cid);
                proto.push_out(PvaMsgTy::ChannelGetInit(ChannelRequestInit {
                    server_cid: res.server_cid,
                    request_id: REQUEST_ID,
                    pv_request: pv_request.clone(),
                }));
            }
            PvaMsgTy::ChannelGetInitRes(res) => {
                if res.request_id != REQUEST_ID {
                    continue;
                }
                if res.status.kind != StatusKind::Ok && res.status.kind != StatusKind::Warning {
                    eprintln!("get init failed: {}", res.status.message);
                    return Err(Error::LogicError);
                }
                let sc = server_cid.ok_or(Error::LogicError)?;
                proto.push_out(PvaMsgTy::ChannelGet(ChannelRequestOp {
                    server_cid: sc,
                    request_id: REQUEST_ID,
                }));
            }
            PvaMsgTy::ChannelGetRes(res) => {
                if res.request_id != REQUEST_ID {
                    continue;
                }
                if res.status.kind != StatusKind::Ok && res.status.kind != StatusKind::Warning {
                    eprintln!("get failed: {}", res.status.message);
                    return Err(Error::LogicError);
                }
                let delta = res.delta.ok_or(Error::LogicError)?;
                let full = delta.into_full().ok_or(Error::LogicError)?;
                if let Some(sc) = server_cid {
                    proto.push_out(PvaMsgTy::DestroyRequest(ChannelRequestOp {
                        server_cid: sc,
                        request_id: REQUEST_ID,
                    }));
                    proto.push_out(PvaMsgTy::DestroyChannelReq(
                        daqbuf_ca_proto::pva::proto::DestroyChannel {
                            server_cid: sc,
                            client_cid: CLIENT_CID,
                        },
                    ));
                }
                return Ok(full.to_json_value());
            }
            _ => {}
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let opts = Opts::parse();
    let fut = run(&opts);
    match timeout(Duration::from_secs(opts.timeout_secs), fut).await {
        Ok(Ok(json)) => {
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
            ExitCode::SUCCESS
        }
        Ok(Err(e)) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
        Err(_) => {
            eprintln!("error: timed out after {}s", opts.timeout_secs);
            ExitCode::FAILURE
        }
    }
}
