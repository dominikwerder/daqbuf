use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use clap::Parser;
use daqbuf_daqretrieve::cli::ClientType;
use daqbuf_daqretrieve::cli::Opts;
use daqbuf_daqretrieve::cli::SubCmd;
use daqbuf_err::Error;
use netpod::NodeConfig;
use netpod::NodeConfigCached;
use netpod::ProxyConfig;
use netpod::ScalarType;
use netpod::ServiceVersion;
use netpod::Shape;
use netpod::log::*;
use netpod::query::CacheUsage;
use taskrun::tokio;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tracing::Instrument;

pub fn main() {
    match taskrun::run(go()) {
        Ok(()) => {}
        Err(k) => {
            error!("{:?}", k);
        }
    }
}

fn parse_ts_rel(s: &str) -> Result<DateTime<Utc>, Error> {
    let (sign, rem) = if s.starts_with("p") { (1, &s[1..]) } else { (-1, s) };
    let (fac, rem) = if rem.ends_with("h") {
        (1000 * 60 * 60, &rem[..rem.len() - 1])
    } else if rem.ends_with("m") {
        (1000 * 60, &rem[..rem.len() - 1])
    } else if rem.ends_with("s") {
        (1000, &rem[..rem.len() - 1])
    } else {
        return Err(Error::with_msg(format!("can not understand relative time: {}", s)))?;
    };
    if rem.contains(".") {
        let num: f32 = rem.parse()?;
        let dur = Duration::milliseconds((num * fac as f32 * sign as f32) as i64);
        Ok(Utc::now() + dur)
    } else {
        let num: i64 = rem.parse()?;
        let dur = Duration::milliseconds(num * fac * sign);
        Ok(Utc::now() + dur)
    }
}

fn parse_ts(s: &str) -> Result<DateTime<Utc>, Error> {
    let ret = if s.contains("-") { s.parse()? } else { parse_ts_rel(s)? };
    Ok(ret)
}

async fn go() -> Result<(), Error> {
    let buildmark = "+0008";
    let opts = Opts::parse();
    let service_version = ServiceVersion {
        major: std::env!("CARGO_PKG_VERSION_MAJOR").parse().unwrap_or(0),
        minor: std::env!("CARGO_PKG_VERSION_MINOR").parse().unwrap_or(0),
        patch: std::env!("CARGO_PKG_VERSION_PATCH").parse().unwrap_or(0),
        pre: std::option_env!("CARGO_PKG_VERSION_PRE").map(Into::into),
    };
    match opts.subcmd {
        SubCmd::Retrieval(subcmd) => {
            info!("daqbuffer  version {} {} retrieval", clap::crate_version!(), buildmark);
            info!("   service_version {} {} retrieval", service_version, buildmark);
            if false {
                #[allow(non_snake_case)]
                let TARGET = std::env!("DAQBUF_TARGET");
                #[allow(non_snake_case)]
                let CARGO_ENCODED_RUSTFLAGS = std::env!("DAQBUF_CARGO_ENCODED_RUSTFLAGS");
                info!("TARGET: {:?}", TARGET);
                info!("CARGO_ENCODED_RUSTFLAGS: {:?}", CARGO_ENCODED_RUSTFLAGS);
            }
            let mut config_file = File::open(&subcmd.config).await?;
            let mut buf = Vec::new();
            config_file.read_to_end(&mut buf).await?;
            if let Ok(cfg) = serde_json::from_slice::<NodeConfig>(b"nothing") {
                info!("Parsed json config from {}", subcmd.config);
                let cfg: Result<NodeConfigCached, netpod::Error> = cfg.into();
                let cfg = cfg.map_err(Error::from_string)?;
                daqbufp2::run_node(cfg, service_version).await?;
            } else if let Ok(cfg) = serde_yaml::from_slice::<NodeConfig>(&buf) {
                info!("Parsed yaml config from {}", subcmd.config);
                let cfg: Result<NodeConfigCached, netpod::Error> = cfg.into();
                let cfg = cfg.map_err(Error::from_string)?;
                daqbufp2::run_node(cfg, service_version).await?;
            } else {
                return Err(Error::with_msg_no_trace(format!(
                    "can not parse config at {}",
                    subcmd.config
                )));
            }
        }
        SubCmd::Proxy(subcmd) => {
            info!("daqbuffer  version {} {} proxy", clap::crate_version!(), buildmark);
            info!("   service_version {} {} proxy", service_version, buildmark);
            let mut config_file = File::open(&subcmd.config).await?;
            let mut buf = Vec::new();
            config_file.read_to_end(&mut buf).await?;
            let proxy_config: ProxyConfig =
                serde_yaml::from_slice(&buf).map_err(|e| Error::with_msg_no_trace(e.to_string()))?;
            info!("Parsed yaml config from {}", subcmd.config);
            daqbufp2::run_proxy(proxy_config.clone(), service_version).await?;
        }
        SubCmd::Client(client) => match client.client_type {
            ClientType::Status(opts) => {
                println!("daqbufp2 not available")
                // daqbufp2::client::status(opts.host, opts.port).await?;
            }
            ClientType::Binned(opts) => {
                let beg = parse_ts(&opts.beg)?;
                let end = parse_ts(&opts.end)?;
                let cache_usage = CacheUsage::from_string(&opts.cache).map_err(Error::from_string)?;
                println!("daqbufp2 not available")
                // daqbufp2::client::get_binned(
                //     opts.host,
                //     opts.port,
                //     opts.backend,
                //     opts.channel,
                //     beg,
                //     end,
                //     opts.bins,
                //     cache_usage,
                //     opts.disk_stats_every_kb,
                // )
                // .await?;
            }
            ClientType::CborEvents(opts) => {
                daqbuf_daqretrieve::fetch::fetch_cbor(
                    &opts.url,
                    ScalarType::from_variant_str(&opts.scalar_type).unwrap(),
                    Shape::from_dims_str(&opts.shape).unwrap(),
                )
                .await
                .map_err(|e| Error::with_msg_no_trace(format!("got error: {e}")))
                .unwrap();
            }
        },
        SubCmd::GenerateTestData => {
            disk::datagen::gen_test_data().await?;
        }
        SubCmd::Test => (),
        SubCmd::Version => {
            println!("{}", clap::crate_version!());
        }
        SubCmd::TestLog => {
            test_log().await;
        }
    }
    Ok(())
}

// TODO
async fn test_log() {
    // daqbufp2::test_log().await;
    // let logspan = tracing::span!(tracing::Level::INFO, "log_span_debug", spanlevel = "info");
    // daqbufp2::test_log().instrument(logspan).await;
    // let logspan = tracing::span!(tracing::Level::INFO, "log_span_debug", spanlevel = "trace");
    // daqbufp2::test_log().instrument(logspan).await;
    // let logspan = tracing::span!(tracing::Level::TRACE, "log_span_trace", spanlevel = "info");
    // daqbufp2::test_log().instrument(logspan).await;
    // let logspan = tracing::span!(tracing::Level::TRACE, "log_span_trace", spanlevel = "trace");
    // daqbufp2::test_log().instrument(logspan).await;
}

// TODO test data needs to be generated.
// TODO use httpclient for the request: need to add binary POST.
//#[test]
#[allow(unused)]
#[cfg(feature = "DISABLED")]
fn simple_fetch() {
    use daqbuffer::err::ErrConv;
    use netpod::ByteOrder;
    use netpod::ScalarType;
    use netpod::SfDbChannel;
    use netpod::Shape;
    use netpod::timeunits::*;
    taskrun::run(async {
        let _rh = daqbufp2::nodes::require_test_hosts_running()?;
        let t1 = chrono::Utc::now();
        let query = AggQuerySingleChannel {
            channel_config: SfDbChConf {
                channel: SfDbChannel::from_name("sf-databuffer", "S10BC01-DBAM070:BAM_CH1_NORM"),
                keyspace: 3,
                time_bin_size: DtNano::from_ns(DAY),
                array: true,
                scalar_type: ScalarType::F64,
                shape: Shape::Wave(42),
                byte_order: ByteOrder::Big,
                compression: true,
            },
            timebin: 18720,
            tb_file_count: 1,
            buffer_size: 1024 * 8,
        };
        let query_string = serde_json::to_string(&query).unwrap();
        let req = hyper::Request::builder()
            .method(http::Method::POST)
            .uri("http://localhost:8360/api/4/parsed_raw")
            .body(query_string.into())
            .ec()?;
        let client = hyper::Client::new();
        let res = client.request(req).await.ec()?;
        info!("client response {:?}", res);
        let mut res_body = res.into_body();
        use hyper::body::HttpBody;
        let mut ntot = 0 as u64;
        loop {
            match res_body.data().await {
                Some(Ok(k)) => {
                    //info!("packet..  len {}", k.len());
                    ntot += k.len() as u64;
                }
                Some(Err(e)) => {
                    error!("{:?}", e);
                }
                None => {
                    info!("response stream exhausted");
                    break;
                }
            }
        }
        let t2 = chrono::Utc::now();
        let ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
        let throughput = ntot / 1024 * 1000 / ms;
        info!(
            "total download {} MB   throughput {:5} kB/s",
            ntot / 1024 / 1024,
            throughput
        );
        Ok::<_, Error>(())
    })
    .unwrap();
}
