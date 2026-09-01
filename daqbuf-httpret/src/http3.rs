use bytes::Bytes;
use h3::server::RequestStream;
use h3_quinn::BidiStream;
use h3_quinn::quinn::crypto::rustls::QuicServerConfig;
use http::Request;
use http::StatusCode;
use netpod::log;
use quinn;
use quinn::Endpoint;
use quinn::EndpointConfig;
use quinn::Incoming;
use rustls::pki_types::pem::PemObject;
use rustls::server::ProducesTickets;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use taskrun::tokio;

const EARLY_DATA_MAX: u32 = u32::MAX * 0;

macro_rules! debug { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ); }

#[derive(Debug)]
struct TicketerCustom {}

impl ProducesTickets for TicketerCustom {
    fn enabled(&self) -> bool {
        false
    }

    fn lifetime(&self) -> u32 {
        60 * 60 * 24
    }

    fn encrypt(&self, _plain: &[u8]) -> Option<Vec<u8>> {
        todo!()
    }

    fn decrypt(&self, _cipher: &[u8]) -> Option<Vec<u8>> {
        todo!()
    }
}

autoerr::create_error_v1!(
    name(Error, "Http3Support"),
    enum variants {
        NoRuntime,
        IO(#[from] std::io::Error),
        H3Connection(#[from] h3::error::ConnectionError),
        H3Stream(#[from] h3::error::StreamError),
        Http(#[from] http::Error),
        Pem(#[from] rustls::pki_types::pem::Error),
        Rustls(#[from] rustls::Error),
        NoInitialCipherSuite(#[from] quinn::crypto::rustls::NoInitialCipherSuite),
        QuinnConnection(#[from] quinn::ConnectionError),
        RingInstallDefault,
    },
);

pub struct Http3Support {
    ep: Option<Endpoint>,
}

impl Http3Support {
    pub async fn new_or_dummy(bind_addr: SocketAddr) -> Result<Self, Error> {
        Ok(Self::new(bind_addr).await.unwrap_or_else(|e| {
            debug!("error {}", e);
            Self::dummy()
        }))
    }

    pub fn dummy() -> Self {
        Self { ep: None }
    }

    async fn new(bind_addr: SocketAddr) -> Result<Self, Error> {
        let key = PemObject::from_pem_file("key.pem")?;
        let cert = PemObject::from_pem_file("cert.pem")?;
        let mut provider = rustls::crypto::ring::default_provider();
        debug!("provider default {:?}", provider);
        provider.cipher_suites = rustls::crypto::ring::ALL_CIPHER_SUITES.to_vec();
        provider.kx_groups = rustls::crypto::ring::ALL_KX_GROUPS.to_vec();
        debug!("provider custom  {:?}", provider);
        provider.install_default().map_err(|_| Error::RingInstallDefault)?;
        let mut tls_conf = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)?;
        tls_conf.alpn_protocols = vec![b"h3".to_vec()];
        tls_conf.max_early_data_size = EARLY_DATA_MAX;
        tls_conf.ticketer = Arc::new(TicketerCustom {});
        let tls_conf = tls_conf;
        let v = QuicServerConfig::try_from(tls_conf)?;
        let quic_conf = Arc::new(v);
        let mut conf_srv = quinn::ServerConfig::with_crypto(quic_conf);
        let mut transport = quinn::TransportConfig::default();
        transport.max_idle_timeout(Some(quinn::VarInt::from_u32(1000 * 10).into()));
        transport.keep_alive_interval(Some(Duration::from_millis(1000 * 2)));
        transport.send_window(quinn::VarInt::from_u32(1024 * 1024 * 8).into());
        transport.receive_window(quinn::VarInt::from_u32(1024 * 1024 * 8).into());
        transport.max_concurrent_bidi_streams(quinn::VarInt::from_u32(100).into());
        let transport = Arc::new(transport);
        conf_srv.transport_config(transport);
        let ep2 = Endpoint::server(conf_srv, bind_addr)?;
        tokio::task::spawn(Self::accept(ep2.clone()));
        let ret = Self { ep: Some(ep2) };
        Ok(ret)
    }

    #[allow(unused)]
    async fn new_plain_quic(bind_addr: SocketAddr) -> Result<Self, Error> {
        let key = PemObject::from_pem_file("key.pem")?;
        let cert = PemObject::from_pem_file("cert.pem")?;
        let conf = EndpointConfig::default();
        let mut tls_conf = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)?;
        tls_conf.alpn_protocols = vec![b"h3".to_vec()];
        tls_conf.max_early_data_size = EARLY_DATA_MAX;
        let tls_conf = tls_conf;
        let v = QuicServerConfig::try_from(tls_conf)?;
        let quic_conf = Arc::new(v);
        let conf_srv = quinn::ServerConfig::with_crypto(quic_conf);
        let sock = std::net::UdpSocket::bind(bind_addr)?;
        debug!("h3 sock {:?}", sock);
        let rt = quinn::default_runtime().ok_or_else(|| Error::NoRuntime)?;
        let ep1 = Endpoint::new(conf, Some(conf_srv.clone()), sock, rt)?;
        tokio::task::spawn(Self::accept(ep1.clone()));
        let ret = Self { ep: Some(ep1) };
        Ok(ret)
    }

    pub async fn wait_idle(self) -> () {
        if let Some(ep) = self.ep.as_ref() {
            ep.close(quinn::VarInt::from_u32(1), b"shutdown");
            ep.wait_idle().await
        }
    }

    async fn accept(ep: Endpoint) {
        debug!("accepting h3");
        while let Some(inc) = ep.accept().await {
            let addr_remote = inc.remote_address();
            tokio::spawn(Self::handle_incoming(inc, addr_remote));
        }
        debug!("accepting h3  RETURN");
    }

    async fn handle_incoming(inc: Incoming, addr_remote: SocketAddr) -> Result<(), Error> {
        debug!("handle_incoming  {}", addr_remote);
        match Self::handle_incoming_inner_2(inc, addr_remote).await {
            Ok(x) => Ok(x),
            Err(e) => {
                debug!("handle_incoming_inner_2 returns error {}", e);
                Err(e)
            }
        }
    }

    #[allow(unused)]
    async fn handle_incoming_inner_1(inc: Incoming, addr_remote: SocketAddr) -> Result<(), Error> {
        debug!("handle_incoming_inner_1  new incoming {:?}", addr_remote);
        let conn1 = inc.accept()?.await?;
        let conn2 = h3_quinn::Connection::new(conn1);
        let mut conn3 = h3::server::builder().build::<_, Bytes>(conn2).await?;
        while let Some(reqres) = conn3.accept().await? {
            let (req, mut stream) = reqres.resolve_request().await?;
            let (head, _body) = req.into_parts();
            debug!(
                "see request  {}  {:?}  {:?}  {:?}",
                addr_remote, head.method, head.uri, head.headers
            );
            let res = http::Response::builder()
                .version(http::Version::HTTP_3)
                .status(StatusCode::OK)
                .header("x-daqbuf-tmp", "8e4b217")
                .body(())?;
            stream.send_response(res).await?;
            stream.send_data(Bytes::from_static(b"2025-02-05T16:37:12Z")).await?;
            stream.finish().await?;
            debug!("response sent  {}", addr_remote);
        }
        Ok(())
    }

    #[allow(unused)]
    async fn handle_incoming_inner_2(inc: Incoming, addr_remote: SocketAddr) -> Result<(), Error> {
        let selfname = "handle_incoming_inner_2";
        debug!("{}  new incoming {:?}", selfname, addr_remote);
        let conn1 = inc.await?;
        debug!("{}  connected  {:?}", selfname, addr_remote);
        let conn2 = h3_quinn::Connection::new(conn1);
        let mut conn3 = h3::server::Connection::new(conn2).await?;
        debug!("{}  h3 Connection awaited  {:?}", selfname, addr_remote);
        // let mut conn3 = h3::server::builder().build::<_, Bytes>(conn2).await?;
        loop {
            break match conn3.accept().await {
                Ok(Some(reqres)) => {
                    let (req, mut stream) = reqres.resolve_request().await?;
                    debug!("in h3 loop  req {:?}", req);
                    tokio::spawn(async move {
                        let x = Self::handle_req(req, stream, addr_remote).await;
                        debug!("handle_req  return {:?}", x);
                    });
                    continue;
                }
                Ok(None) => {
                    debug!("in h3 loop None");
                }
                Err(e) => {
                    debug!("in h3 loop error {}", e);
                }
            };
        }
        Ok(())
    }

    async fn handle_req(
        req: Request<()>,
        mut stream: RequestStream<BidiStream<Bytes>, Bytes>,
        addr_remote: SocketAddr,
    ) -> Result<(), Error> {
        let method = req.method();
        let uri = req.uri();
        let headers = req.headers();
        debug!("see request  {}  {:?}  {:?}  {:?}", addr_remote, method, uri, headers);
        let res = http::Response::builder()
            // .version(http::Version::HTTP_3)
            .status(StatusCode::OK)
            .header("x-daqbuf-http3", "1")
            .header("content-type", "text/plain")
            .body(())?;
        stream.send_response(res).await?;
        // stream.send_data(res).await?;
        let s = format!("{:?}", std::time::SystemTime::now());
        let b = s.as_bytes().to_vec();
        stream.send_data(Bytes::from(b)).await?;
        stream.finish().await?;
        debug!("response sent  {}", addr_remote);
        Ok(())
    }
}
