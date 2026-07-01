use crate::err::ErrConv;
use chrono::DateTime;
use chrono::Utc;
use daqbuf_err::Error;
use disk::streamlog::Streamlog;
use futures_util::TryStreamExt;
use http::StatusCode;
use http::Uri;
use httpclient::body_empty;
use items_0::streamitem::StreamItem;
use netpod::log::*;
use netpod::query::CacheUsage;
use netpod::range::evrange::NanoRange;
use netpod::AppendToUrl;
use netpod::ByteSize;
use netpod::HostPort;
use netpod::SfDbChannel;
use netpod::APP_OCTET;
use query::api4::binned::BinnedQuery;
use streams::frames::inmem::InMemoryFrameStream;
use url::Url;

pub async fn status(host: String, port: u16) -> Result<(), Error> {
    let t1 = Utc::now();
    let uri = format!("http://{}:{}/api/4/node_status", host, port,);
    let uri: Uri = uri.parse()?;
    let req = http::Request::builder()
        .method(http::Method::GET)
        .header(http::header::HOST, uri.host().unwrap())
        .uri(uri)
        .body(body_empty())?;
    let mut client = httpclient::connect_client(req.uri()).await?;
    let res = client.send_request(req).await.map_err(Error::from_string)?;
    if res.status() != StatusCode::OK {
        error!("Server error  {:?}", res);
        return Err(Error::with_msg(format!("Server error  {:?}", res)));
    }
    let (_, body) = res.into_parts();
    let body = httpclient::read_body_bytes(body).await?;
    let res = String::from_utf8_lossy(&body);
    let t2 = chrono::Utc::now();
    let ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
    info!("node_status DONE  duration: {} ms", ms);
    println!("{}", res);
    Ok(())
}

pub async fn get_binned(
    host: String,
    port: u16,
    channel_backend: String,
    channel_name: String,
    beg_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    bin_count: u32,
    cache_usage: CacheUsage,
    disk_stats_every_kb: u32,
) -> Result<(), Error> {
    info!("-------   get_binned  client");
    info!("channel {}", channel_name);
    info!("beg  {}", beg_date);
    info!("end  {}", end_date);
    info!("-------");
    let t1 = Utc::now();
    let channel = SfDbChannel::from_name(channel_backend, channel_name);
    let range = NanoRange::from_date_time(beg_date, end_date).into();
    // TODO this was before fixed using AggKind::DimXBins1
    let mut query = BinnedQuery::new(channel, range, bin_count).for_time_weighted_scalar();
    query.set_cache_usage(cache_usage);
    query.set_disk_stats_every(ByteSize(1024 * disk_stats_every_kb));
    let hp = HostPort { host: host, port: port };
    let mut url = Url::parse(&format!("http://{}:{}/api/4/binned", hp.host, hp.port))?;
    query.append_to_url(&mut url);
    let url = url;
    let uri: Uri = url.as_str().parse()?;
    let req = http::Request::builder()
        .method(http::Method::GET)
        .header(http::header::HOST, uri.host().unwrap())
        .header(http::header::ACCEPT, APP_OCTET)
        .uri(uri)
        .body(body_empty())
        .ec()?;
    let mut client = httpclient::connect_client(req.uri()).await?;
    let res = client.send_request(req).await.map_err(Error::from_string)?;
    if res.status() != StatusCode::OK {
        error!("Server error  {:?}", res);
        let (head, body) = res.into_parts();
        let body = httpclient::read_body_bytes(body).await?;
        let s = String::from_utf8_lossy(&body);
        return Err(Error::with_msg(format!(
            concat!(
                "Server error  {:?}\n",
                "---------------------- message from http body:\n",
                "{}\n",
                "---------------------- end of http body",
            ),
            head, s
        )));
    }
    let (_head, body) = res.into_parts();
    let inp = httpclient::IncomingStream::new(body);
    let s2 = InMemoryFrameStream::new(inp, ByteSize::from_kb(8));
    use futures_util::StreamExt;
    use std::future::ready;
    let s3 = s2
        .map_err(|e| error!("get_binned  {:?}", e))
        .filter_map(|item| {
            let g = match item {
                Ok(item) => match item {
                    StreamItem::Log(item) => {
                        Streamlog::emit(&item);
                        None
                    }
                    StreamItem::Stats(item) => {
                        info!("Stats: {:?}", item);
                        None
                    }
                    StreamItem::DataItem(_frame) => {
                        // TODO
                        // The expected type nowadays depends on the channel and agg-kind.
                        daqbuf_err::todo();
                        Some(Ok(()))
                    }
                },
                Err(e) => Some(Err(Error::with_msg(format!("{:?}", e)))),
            };
            ready(g)
        })
        .for_each(|_| ready(()));
    s3.await;
    let t2 = chrono::Utc::now();
    let ntot = 0;
    let ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
    let throughput = ntot / 1024 * 1000 / ms;
    info!(
        "get_cached_0 DONE  total download {} MB   throughput {:5} kB/s  bin_count {}",
        ntot / 1024 / 1024,
        throughput,
        bin_count
    );
    Ok(())
}
