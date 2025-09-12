pub mod ioc_conn;

use super::super::connfut::ConnFutResource;
use crate::ca::conn2::statetrans::stateress1::StateRessShr1;
use ioc_conn::IocConn;
use std::net::SocketAddrV4;
use std::time::Instant;

struct RefHolder {
    ptr: *const ConnFutResource,
}

impl RefHolder {
    pub fn get_ref(&self) -> &ConnFutResource {
        todo!()
    }
}

thread_local! {
    static CONN_RES_A: RefHolder = RefHolder{ ptr: std::ptr::null() };
    static CONN_RES_B: RefHolder = const { RefHolder{ ptr: std::ptr::null() } };
}

fn test_ref_holder() {
    CONN_RES_A.with(|x| {
        x.get_ref();
    });
    CONN_RES_B.with(|x| ());
}

struct Resource1 {}

async fn go2(x: &mut String) {}

fn go1<'a>(x: &'a mut String) -> impl Future<Output = ()> + use<> {
    x.push_str("hello");
    async move {
        // go2(x).await;
        // x.push_str("hello");
    }
}

fn use_go() {
    let mut s = String::new();
    let fut1 = go1(&mut s);
    let fut2 = go1(&mut s);
    taskrun::tokio::runtime::Runtime::new().unwrap().block_on(async {});
}

#[derive(Debug)]
pub struct IocConnStateBase {
    ts_beg: Instant,
    remote_addr: SocketAddrV4,
    conn: IocConn,
}

impl IocConnStateBase {
    pub fn new(remote_addr: SocketAddrV4, ress_a: StateRessShr1) -> Self {
        Self {
            ts_beg: Instant::now(),
            conn: IocConn::init(remote_addr.clone(), ress_a),
            remote_addr,
        }
    }
}
