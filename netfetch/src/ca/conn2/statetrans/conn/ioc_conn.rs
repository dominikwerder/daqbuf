use crate::ca::conn2::futstack::ErasedFuture;
use crate::ca::conn2::statetrans::stateress1::StateRessShr1;
use ca_proto::ca::proto::CaProto;
use ca_proto_tokio::tcpasyncwriteread::TcpAsyncWriteRead;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use scywr::iteminsertqueue::QueryItem;
use std::collections::VecDeque;
use std::fmt;
use std::mem;
use std::net::SocketAddrV4;
use std::ops::Deref;
use std::ops::DerefMut;
use std::os::fd::AsFd;
use std::os::fd::AsRawFd;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;
use std::time::SystemTime;
use taskrun::tokio;
use tokio::net::TcpStream;

autoerr::create_error_v1!(
    name(Error, "IocConn"),
    enum variants {
        TimeoutConnect,
        IO(#[from] std::io::Error),
    },
);

impl From<tokio::time::error::Elapsed> for Error {
    fn from(_e: tokio::time::error::Elapsed) -> Self {
        Self::TimeoutConnect
    }
}

type ConnectingFutType =
    Pin<Box<dyn Future<Output = Result<Result<TcpStream, std::io::Error>, tokio::time::error::Elapsed>> + Send>>;

struct ConnectingFut(ConnectingFutType);

impl fmt::Debug for ConnectingFut {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_tuple("ConnectingFut").finish()
    }
}

impl Deref for ConnectingFut {
    type Target = ConnectingFutType;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ConnectingFut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug)]
pub struct Init {
    ts_beg: Instant,
    remote_addr: SocketAddrV4,
    ress_a: StateRessShr1,
}

#[derive(Debug)]
pub struct ConnectingTcp {
    ts_beg: Instant,
    remote_addr: SocketAddrV4,
    ress_a: StateRessShr1,
    fut: ConnectingFut,
}

impl Future for ConnectingTcp {
    type Output = Result<TcpStream, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        loop {
            break match self.fut.poll_unpin(cx) {
                Ready(Ok(Ok(x))) => Ready(Ok(x)),
                Ready(Ok(Err(e))) => Ready(Err(e.into())),
                Ready(Err(e)) => Ready(Err(e.into())),
                Pending => Pending,
            };
        }
    }
}

#[derive(Debug)]
pub struct ConnectingTcpDone {
    ts_beg: Instant,
    remote_addr: SocketAddrV4,
    ress_a: StateRessShr1,
    queue_insert: ErasedFuture<(), 4>,
}

impl ConnectingTcpDone {
    fn new(
        tcp: TcpStream,
        ress_a: StateRessShr1,
        remote_addr: SocketAddrV4,
        queue_insert: ErasedFuture<(), 4>,
    ) -> Self {
        let tsnow = Instant::now();
        Self {
            ts_beg: tsnow,
            remote_addr,
            ress_a,
            queue_insert,
        }
    }
}

impl Future for ConnectingTcpDone {
    type Output = Result<TcpStream, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        // use Poll::*;
        todo!()
    }
}

#[derive(Debug)]
pub struct SendingHandshake {
    ts_beg: Instant,
    proto: CaProto,
    ress_a: StateRessShr1,
    // TODO smal set of messages that we want to send as handshake.
    // TODO requires access to proto.
    // TODO requires &mut self.
    msgs_buf: VecDeque<String>,
}

#[derive(Debug)]
pub struct IocConnReady {
    ts_beg: Instant,
    shutting_down: bool,
    proto: CaProto,
}

#[derive(Debug)]
pub enum IocConn {
    Init(Init),
    ConnectingTcp(ConnectingTcp),
    ConnectingTcpDone(ConnectingTcpDone),
    SendingHandshake(SendingHandshake),
    IocConnReady(IocConnReady),
    ShutdownDoingRemainingTcpStuff,
    ShutdownTcpClosing,
    Done,
    Dummy,
}

impl IocConn {
    pub fn init(remote_addr: SocketAddrV4, ress_a: StateRessShr1) -> Self {
        Self::Init(Init {
            ts_beg: Instant::now(),
            remote_addr,
            ress_a,
        })
    }
}

fn assert_ptr_type<T>(a: *const T, b: *const T) {
    let mut x = a;
    x = b;
    let _ = x;
}

impl Stream for IocConn {
    type Item = Result<(), Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            let self2 = self.as_mut().get_mut();
            break match self2 {
                IocConn::Init(st2) => {
                    let now = Instant::now();
                    let fut = ConnectingFut(Box::pin(tokio::time::timeout(
                        std::time::Duration::from_millis(4000),
                        TcpStream::connect(st2.remote_addr),
                    )));
                    *self = IocConn::ConnectingTcp(ConnectingTcp {
                        ts_beg: now,
                        remote_addr: st2.remote_addr,
                        ress_a: st2.ress_a.clone(),
                        fut,
                    });
                    Pending
                }
                IocConn::ConnectingTcp(st2) => match st2.fut.poll_unpin(cx) {
                    Ready(Ok(Ok(tcp))) => {
                        let stptr = st2 as *const _;
                        if let IocConn::ConnectingTcp(stt) = mem::replace(self2, IocConn::Dummy) {
                            assert_ptr_type(stptr, &stt as *const _);
                            let ress_a = stt.ress_a;
                            let remote_addr = stt.remote_addr;

                            // TODO start future to emit the status item.
                            let fut = {
                                let item = scywr::iteminsertqueue::ConnectionStatusItem {
                                    ts: SystemTime::now(),
                                    addr: remote_addr,
                                    status: scywr::iteminsertqueue::ConnectionStatus::Established,
                                };
                                // TODO connection status requires separate table.
                                // DUMMY:
                                let item = QueryItem::Insert(netpod::todoval());
                                let mut ress_ref = ress_a.borrow_mut();
                                let fut = ress_ref.scy_wr_qus().lt().push(item);
                                ErasedFuture::new(fut)
                            };

                            *self2 = IocConn::ConnectingTcpDone(ConnectingTcpDone::new(tcp, ress_a, remote_addr, fut));
                            continue;
                        } else {
                            // TODO the extra match is not nice.
                            panic!()
                        }
                    }
                    Ready(Ok(Err(e))) => {
                        *self2 = IocConn::Done;
                        Ready(Some(Err(e.into())))
                    }
                    Ready(Err(e)) => {
                        *self2 = IocConn::Done;
                        Ready(Some(Err(e.into())))
                    }
                    Pending => Pending,
                },
                IocConn::ConnectingTcpDone(st2) => match st2.poll_unpin(cx) {
                    Ready(Ok(tcp)) => {
                        let stptr = st2 as *const _;
                        if let IocConn::ConnectingTcpDone(stt) = mem::replace(self2, IocConn::Dummy) {
                            assert_ptr_type(stptr, &stt as *const _);
                            let tsnow = Instant::now();
                            let ress_a = stt.ress_a;
                            let remote_addr = stt.remote_addr;
                            let proto = {
                                let fd = tcp.as_fd().as_raw_fd();
                                CaProto::new(
                                    TcpAsyncWriteRead::from(tcp),
                                    Some(fd),
                                    remote_addr.to_string(),
                                    1024 * 1024 * 16,
                                )
                            };
                            let msgs_buf = VecDeque::new();
                            *self2 = IocConn::SendingHandshake(SendingHandshake {
                                ts_beg: tsnow,
                                proto,
                                msgs_buf,
                                ress_a,
                            });
                            continue;
                        } else {
                            // TODO the extra match is not nice.
                            panic!()
                        }
                    }
                    Ready(Err(e)) => {
                        *self = IocConn::Done;
                        Ready(Some(Err(e)))
                    }
                    Pending => Pending,
                },
                _ => {
                    // TODO
                    Pending
                }
            };
        }
    }
}
