use daqbuf_err as err;
use err::Error;
use futures_util::Future;
use futures_util::TryFutureExt;
use http::Method;
use http::Request;
use http::header;
use httpclient::body_bytes;
use httpclient::http;
use httpclient::hyper::StatusCode;
use httpclient::hyper::Uri;
use items_0::streamitem::sitem_data;
// use items_0::streamitem::sitem_err2_from_string;
use items_2::framable::Framable;
use netpod::APP_OCTET;
use netpod::Cluster;
use netpod::ReqCtx;
use netpod::log::*;
use query::api4::events::EventsSubQuery;
use std::pin::Pin;
use streams::frames::inmem::BoxedBytesStream;
use streams::tcprawclient::OpenBoxedBytesStreams;
use streams::tcprawclient::make_node_command_frame;

async fn open_bytes_data_streams_http(
    subq: EventsSubQuery,
    ctx: ReqCtx,
    cluster: Cluster,
) -> Result<Vec<BoxedBytesStream>, Error> {
    let frame1 = make_node_command_frame(subq.clone()).map_err(|e| Error::from_string(e))?;
    let mut streams = Vec::new();
    for node in &cluster.nodes {
        let item = sitem_data(frame1.clone());
        let buf = item.make_frame_dyn().map_err(|e| Error::from_string(e))?;

        let url = node.baseurl().join("/api/4/private/eventdata/frames").unwrap();
        debug!("open_event_data_streams_http  post  {url}");
        let uri: Uri = url.as_str().parse().unwrap();
        let req = Request::builder()
            .method(Method::POST)
            .uri(&uri)
            .header(header::HOST, uri.host().unwrap())
            .header(header::ACCEPT, APP_OCTET)
            .header(ctx.header_name(), ctx.header_value())
            .body(body_bytes(buf))
            .map_err(|e| Error::with_msg_no_trace(e.to_string()))?;
        let mut client = httpclient::connect_client(req.uri()).await?;
        let res = client
            .send_request(req)
            .await
            .map_err(|e| Error::with_msg_no_trace(e.to_string()))?;
        if res.status() != StatusCode::OK {
            error!("Server error  {:?}", res);
            let (head, body) = res.into_parts();
            let buf = httpclient::read_body_bytes(body).await?;
            let s = String::from_utf8_lossy(&buf);
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
        let stream = Box::pin(httpclient::IncomingStream::new(body)) as BoxedBytesStream;
        debug!("open_event_data_streams_http  done  {url}");
        streams.push(Box::pin(stream) as _);
    }
    Ok(streams)
}

pub struct OpenBoxedBytesViaHttp {
    cluster: Cluster,
}

impl OpenBoxedBytesViaHttp {
    pub fn new(cluster: Cluster) -> Self {
        Self { cluster }
    }
}

impl OpenBoxedBytesStreams for OpenBoxedBytesViaHttp {
    fn open(
        &self,
        subq: EventsSubQuery,
        ctx: ReqCtx,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<BoxedBytesStream>, streams::tcprawclient::Error>> + Send>> {
        let fut = open_bytes_data_streams_http(subq, ctx, self.cluster.clone());
        let fut = fut.map_err(|e| streams::tcprawclient::Error::from(e.to_string()));
        Box::pin(fut)
    }
}
