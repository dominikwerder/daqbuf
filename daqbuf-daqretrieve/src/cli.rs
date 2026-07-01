use clap::ArgAction;
use clap::Parser;

#[derive(Debug, Parser)]
#[command(author, version)]
pub struct Opts {
    #[arg(long, action(ArgAction::Count))]
    pub verbose: u8,
    #[command(subcommand)]
    pub subcmd: SubCmd,
}

#[derive(Debug, Parser)]
pub enum SubCmd {
    Retrieval(Retrieval),
    Proxy(Proxy),
    Client(Client),
    GenerateTestData,
    Test,
    Version,
    TestLog,
}

#[derive(Debug, Parser)]
pub struct Retrieval {
    #[arg(long)]
    pub config: String,
}

#[derive(Debug, Parser)]
pub struct Proxy {
    #[arg(long)]
    pub config: String,
}

#[derive(Debug, Parser)]
pub struct Client {
    #[command(subcommand)]
    pub client_type: ClientType,
}

#[derive(Debug, Parser)]
pub enum ClientType {
    Binned(BinnedClient),
    Status(StatusClient),
    CborEvents(CborEvents),
}

#[derive(Debug, Parser)]
pub struct StatusClient {
    #[arg(long)]
    pub host: String,
    #[arg(long)]
    pub port: u16,
}

#[derive(Debug, Parser)]
pub struct BinnedClient {
    #[arg(long)]
    pub host: String,
    #[arg(long)]
    pub port: u16,
    #[arg(long)]
    pub backend: String,
    #[arg(long)]
    pub channel: String,
    #[arg(long)]
    pub beg: String,
    #[arg(long)]
    pub end: String,
    #[arg(long)]
    pub bins: u32,
    #[arg(long, default_value = "use")]
    pub cache: String,
    #[arg(long, default_value = "1048576")]
    pub disk_stats_every_kb: u32,
}

#[derive(Debug, Parser)]
pub struct CborEvents {
    #[arg(long)]
    pub url: String,
    #[arg(long)]
    pub scalar_type: String,
    #[arg(long)]
    pub shape: String,
}
