use ca_proto_tokio::tcpasyncwriteread::TcpAsyncWriteRead;
use clap::Parser;
use daqbuf_ca_proto::pva::client;
use daqbuf_ca_proto::pva::proto::PvaRequest;
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

async fn run(opts: &Opts) -> Result<serde_json::Value, daqbuf_ca_proto::pva::Error> {
    let tcp = TcpStream::connect((opts.host.as_str(), opts.port)).await?;
    tcp.set_nodelay(true).ok();
    let remote_name = format!("{}:{}", opts.host, opts.port);
    let tcp = TcpAsyncWriteRead::from(tcp);
    let pv_request = match &opts.request {
        Some(s) => PvaRequest::parse(s)?,
        None => PvaRequest::all(),
    };
    let full = client::get_once(tcp, remote_name, &opts.pv, pv_request, 1024 * 1024).await?;
    Ok(full.to_json_value())
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
