use crate::bodystream::response;
use async_channel::Receiver;
use async_channel::Sender;
use bytes::Bytes;
use daqbuf_err as err;
use err::PublicError;
use err::ToPublicError;
use futures_util::Stream;
use futures_util::StreamExt;
use http::Method;
use http::Response;
use http::StatusCode;
use httpclient::body_empty;
use httpclient::body_stream;
use httpclient::error_response;
use httpclient::Requ;
use httpclient::StreamResponse;
use netpod::log::*;
use netpod::req_uri_to_url;
use netpod::Node;
use netpod::NodeConfigCached;
use netpod::ACCEPT_ALL;
use netpod::APP_JSON;
use serde::Serialize;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use taskrun::tokio;

autoerr::create_error_v1!(
    name(FindActiveError, "ChannelFindActive"),
    enum variants {
        HttpBadAccept,
        HttpBadUrl,
        Error(Box<dyn ToPublicError>),
        UrlError(#[from] url::ParseError),
        InternalError,
        IO(#[from] std::io::Error),
    },
);

impl ToPublicError for FindActiveError {
    fn to_public_error(&self) -> PublicError {
        match self {
            FindActiveError::HttpBadAccept => format!("{self}").into(),
            FindActiveError::HttpBadUrl => format!("{self}").into(),
            FindActiveError::Error(e) => e.to_public_error(),
            FindActiveError::UrlError(_) => format!("{self}").into(),
            FindActiveError::InternalError => format!("{self}").into(),
            FindActiveError::IO(_) => format!("{self}").into(),
        }
    }
}

pub struct FindActiveHandler {}

impl FindActiveHandler {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == "/api/4/tool/sfdatabuffer/find/channel/active" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Requ, ncc: &NodeConfigCached) -> Result<StreamResponse, FindActiveError> {
        if req.method() != Method::GET {
            Ok(response(StatusCode::NOT_ACCEPTABLE)
                .body(body_empty())
                .map_err(|_| FindActiveError::InternalError)?)
        } else {
            match Self::handle_req(req, ncc).await {
                Ok(ret) => Ok(ret),
                Err(e) => {
                    error!("{e}");
                    let res = error_response(e.to_public_error().to_string(), "missing-req");
                    Ok(res)
                }
            }
        }
    }

    async fn handle_req(req: Requ, ncc: &NodeConfigCached) -> Result<StreamResponse, FindActiveError> {
        let accept_def = APP_JSON;
        let accept = req
            .headers()
            .get(http::header::ACCEPT)
            .map_or(accept_def, |k| k.to_str().unwrap_or(accept_def));
        let _url = req_uri_to_url(req.uri()).map_err(|_| FindActiveError::HttpBadUrl)?;
        if accept.contains(APP_JSON) || accept.contains(ACCEPT_ALL) {
            let stream = FindActiveStream::new(40, 2, ncc);
            let stream = stream.chain(FindActiveStream::new(40, 3, ncc));
            let stream = stream.chain(FindActiveStream::new(40, 4, ncc));
            let stream = stream
                .map(|item| match item {
                    Ok(item) => {
                        let mut s = serde_json::to_vec(&item).unwrap();
                        s.push(0x0a);
                        s
                    }
                    Err(e) => {
                        error!("ERROR in http body stream after headers: {e}");
                        Vec::new()
                    }
                })
                .map(|x| Ok::<_, String>(Bytes::from(x)));
            let body = body_stream(stream);
            Ok(Response::builder().status(StatusCode::OK).body(body).unwrap())
        } else {
            Err(FindActiveError::HttpBadAccept)
        }
    }
}

#[derive(Debug, Serialize)]
struct ActiveChannelDesc {
    ks: u32,
    name: String,
    totlen: u64,
}

async fn sum_dir_contents(path: PathBuf) -> Result<u64, FindActiveError> {
    let mut sum = 0;
    let mut dir_stream = tokio::fs::read_dir(path).await?;
    loop {
        match dir_stream.next_entry().await? {
            Some(x) => {
                if x.file_name().to_string_lossy().starts_with("..") {
                    debug!("INCONVENIENT: {x:?}");
                } else if x.file_type().await.unwrap().is_dir() {
                    let mut dir_stream_2 = tokio::fs::read_dir(x.path()).await?;
                    loop {
                        match dir_stream_2.next_entry().await? {
                            Some(x) => {
                                let md = x.metadata().await?;
                                sum += md.len();
                            }
                            None => break,
                        }
                    }
                } else {
                    error!("unexpected file: {:?}", x.file_name());
                    sum += x.metadata().await?.len();
                }
            }
            None => break,
        }
    }
    Ok(sum)
}

struct XorShift32 {
    state: u32,
}

impl XorShift32 {
    fn new(state: u32) -> Self {
        Self { state }
    }

    fn next(&mut self) -> u32 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        self.state = x;
        x
    }
}

async fn find_active_inner(
    max: usize,
    ks: u32,
    _splits: &[u64],
    node: Node,
    tx: Sender<Result<ActiveChannelDesc, FindActiveError>>,
) -> Result<(), FindActiveError> {
    let mut count = 0;
    let now_sec = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut rng = XorShift32::new(now_sec as u32);
    for _ in 0..64 {
        rng.next();
    }
    let tb_exp_dat = now_sec / 60 / 60 / 24;
    let tb_exp_img = now_sec / 60 / 60;
    let re_tb = regex::Regex::new(r"(0000\d{15})").unwrap();
    let path = disk::paths::datapath_for_keyspace(ks, &node);
    let mut dir_stream = tokio::fs::read_dir(path).await?;
    let mut channel_dirs = Vec::new();
    loop {
        let x = dir_stream.next_entry().await?;
        match x {
            Some(x) => {
                if x.file_name().to_string_lossy().starts_with(".") {
                    debug!("INCONVENIENT: {x:?}");
                } else if x.file_name().to_string_lossy().starts_with("..") {
                    debug!("INCONVENIENT: {x:?}");
                } else {
                    channel_dirs.push((rng.next(), x));
                }
            }
            None => break,
        }
    }
    channel_dirs.sort_by_key(|x| x.0);
    let channel_dirs = channel_dirs;
    // TODO randomize channel list using given seed
    'outer: for (_, chdir) in channel_dirs {
        let ft = chdir.file_type().await?;
        if ft.is_dir() {
            let mut dir_stream = tokio::fs::read_dir(chdir.path())
                .await
                .map_err(|e| FindActiveError::IO(e))?;
            loop {
                match dir_stream.next_entry().await? {
                    Some(e) => {
                        let x = e.file_name();
                        let s = x.to_string_lossy();
                        if let Some(_) = re_tb.captures(&s) {
                            let chn1 = chdir.file_name();
                            let chname = chn1.to_string_lossy();
                            // debug!("match: {m:?}");
                            // TODO bin-size depends on channel config
                            match s.parse::<u64>() {
                                Ok(x) => {
                                    if (ks == 2 || ks == 3) && x == tb_exp_dat || ks == 4 && x == tb_exp_img {
                                        // debug!("matching tb {}", chname);
                                        let sum = sum_dir_contents(e.path()).await?;
                                        if sum > 1024 * 1024 * 10 {
                                            // debug!("sizable content: {sum}");
                                            let x = ActiveChannelDesc {
                                                ks,
                                                name: chname.into(),
                                                totlen: sum,
                                            };
                                            if let Err(e) = tx.send(Ok(x)).await {
                                                error!("{e}");
                                            }
                                            count += 1;
                                            if count >= max {
                                                break 'outer;
                                            }
                                        }
                                    }
                                }
                                Err(_) => {}
                            }
                        } else {
                            // debug!("no match");
                        }
                    }
                    None => break,
                }
            }
        } else {
            error!("unexpected file {chdir:?}");
        }
    }
    Ok(())
}

async fn find_active(
    max: usize,
    ks: u32,
    splits: Vec<u64>,
    node: Node,
    tx: Sender<Result<ActiveChannelDesc, FindActiveError>>,
) {
    let tx2 = tx.clone();
    match find_active_inner(max, ks, &splits, node, tx).await {
        Ok(x) => x,
        Err(e) => {
            if let Err(e) = tx2.send(Err(e)).await {
                error!("{e}");
            }
            return;
        }
    }
}

#[pin_project::pin_project]
struct FindActiveStream {
    #[pin]
    rx: Receiver<Result<ActiveChannelDesc, FindActiveError>>,
}

impl FindActiveStream {
    pub fn new(max: usize, ks: u32, ncc: &NodeConfigCached) -> Self {
        let (tx, rx) = async_channel::bounded(4);
        let splits = ncc
            .node
            .sf_databuffer
            .as_ref()
            .unwrap()
            .splits
            .as_ref()
            .map_or(Vec::new(), Clone::clone);
        let _jh = taskrun::spawn(find_active(max, ks, splits, ncc.node.clone(), tx));
        Self { rx }
    }
}

impl Stream for FindActiveStream {
    type Item = Result<ActiveChannelDesc, FindActiveError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.project().rx.poll_next_unpin(cx) {
            Ready(Some(item)) => Ready(Some(item)),
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }
}
