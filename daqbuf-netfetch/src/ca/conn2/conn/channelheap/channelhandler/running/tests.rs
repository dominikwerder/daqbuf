use super::*;
use crate::ca::conn2::caids::Cid;
use crate::ca::conn2::caids::Sid;
use dbpg::seriesbychannel::RegisteredSeries;
use futures::task::noop_waker;
use scywr::channelwriterstate::WriterStateInsert;
use scywr::channelwriterstate::WriterStateSelect;
use series::SeriesId;

const SERIES: u64 = 4242;

fn new_running() -> Running {
    let chconf = ChannelConfig::st_monitor("SOME:CHANNEL", "test.hcl");
    let chi = ChannelInfoResult {
        backend: "testbackend".into(),
        channel: "SOME:CHANNEL".into(),
        series: RegisteredSeries::Current(SeriesId::new(SERIES)),
    };
    Running::new(
        Cid::new(3),
        Sid::new(7),
        ScalarType::F64,
        Shape::Scalar,
        CaDbrTy::new(6),
        chi,
        chconf,
        IoidRegistry::testing(),
    )
    .unwrap()
}

fn poll_once(r: &mut Running) -> Poll<Option<Result<RunningItem, Error>>> {
    let w = noop_waker();
    let mut cx = Context::from_waker(&w);
    Pin::new(r).poll_next(&mut cx)
}

fn drain(r: &mut Running) -> (Vec<RunningItem>, bool) {
    let mut items = Vec::new();
    loop {
        match poll_once(r) {
            Poll::Ready(Some(Ok(x))) => {
                items.push(x);
                assert!(items.len() < 50, "runaway stream  {:?}", &items[..5]);
            }
            Poll::Ready(Some(Err(e))) => panic!("unexpected error {e}"),
            Poll::Ready(None) => break (items, true),
            Poll::Pending => break (items, false),
        }
    }
}

fn take_select(items: &mut Vec<RunningItem>) -> WriterStateSelect {
    let pos = items
        .iter()
        .position(|x| matches!(x, RunningItem::WriterStateQuery(WriterStateQuery::Select(..))))
        .expect("no select emitted");
    match items.remove(pos) {
        RunningItem::WriterStateQuery(WriterStateQuery::Select(x)) => x,
        _ => unreachable!(),
    }
}

fn take_inserts(items: &mut Vec<RunningItem>) -> Vec<WriterStateInsert> {
    let mut ret = Vec::new();
    let mut rest = Vec::new();
    for x in items.drain(..) {
        match x {
            RunningItem::WriterStateQuery(WriterStateQuery::Insert(x)) => ret.push(x),
            x => rest.push(x),
        }
    }
    *items = rest;
    ret
}

fn into_normal(r: &mut Running) {
    let (mut items, _) = drain(r);
    let mut select = take_select(&mut items);
    select.tx().try_send(Ok(vec![0])).ok().unwrap();
    let _ = drain(r);
    assert_eq!(r.state.str(), "Normal");
}

fn with_runtime(f: impl FnOnce()) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let _g = rt.enter();
    f();
}

fn rx_item(ty: proto::CaMsgTy) -> ProtoRxItem {
    let ts = Instant::now();
    ProtoRxItem {
        msg: CaMsg::from_ty_ts(ty, ts),
        tscmd: ts,
        tsreg: ts,
        tsdisp: ts,
    }
}

#[test]
fn emits_select_on_start() {
    with_runtime(|| {
        let mut r = new_running();
        let (mut items, ended) = drain(&mut r);
        assert!(!ended);
        let select = take_select(&mut items);
        assert_eq!(select.series().id(), SERIES);
        assert_eq!(r.state.str(), "WriterStateQuery");
    });
}

#[test]
fn done_from_normal_goes_through_insert() {
    with_runtime(|| {
        let mut r = new_running();
        into_normal(&mut r);
        r.transition_state(State::Done);
        assert_eq!(r.state.str(), "WriterStateInsert");
        let (mut items, ended) = drain(&mut r);
        assert!(!ended);
        let inserts = take_inserts(&mut items);
        assert_eq!(inserts.len(), 1);
        assert_eq!(inserts[0].series().id(), SERIES);
    });
}

#[test]
fn done_from_select_skips_insert() {
    with_runtime(|| {
        let mut r = new_running();
        let (mut items, _) = drain(&mut r);
        let _select = take_select(&mut items);
        r.transition_state(State::Done);
        assert_eq!(r.state.str(), "Done");
        let (mut items, ended) = drain(&mut r);
        assert!(ended);
        assert!(take_inserts(&mut items).is_empty());
    });
}

#[test]
fn done_from_insert_and_done_is_idempotent() {
    with_runtime(|| {
        let mut r = new_running();
        into_normal(&mut r);
        r.transition_state(State::Done);
        r.transition_state(State::Done);
        assert_eq!(r.state.str(), "Done");
        let (mut items, _) = drain(&mut r);
        assert_eq!(take_inserts(&mut items).len(), 1);
        r.transition_state(State::Done);
        assert_eq!(r.state.str(), "Done");
        assert!(r.outbuf.is_empty());
    });
}

#[test]
fn insert_ok_ends_stream() {
    with_runtime(|| {
        let mut r = new_running();
        into_normal(&mut r);
        r.transition_state(State::Done);
        let (mut items, ended) = drain(&mut r);
        assert!(!ended);
        let mut insert = take_inserts(&mut items).pop().unwrap();
        insert.tx().try_send(Ok(())).ok().unwrap();
        let (_, ended) = drain(&mut r);
        assert!(ended);
        assert_eq!(r.state.str(), "Done");
    });
}

#[test]
fn insert_err_does_not_hang() {
    with_runtime(|| {
        let mut r = new_running();
        into_normal(&mut r);
        r.transition_state(State::Done);
        let (mut items, _) = drain(&mut r);
        let mut insert = take_inserts(&mut items).pop().unwrap();
        insert
            .tx()
            .try_send(Err(scywr::channelwriterstate::Error::Logic))
            .ok()
            .unwrap();
        let (_, ended) = drain(&mut r);
        assert!(ended);
        assert_eq!(r.state.str(), "Done");
    });
}

#[test]
fn insert_receiver_closed_does_not_hang() {
    with_runtime(|| {
        let mut r = new_running();
        into_normal(&mut r);
        r.transition_state(State::Done);
        let (mut items, _) = drain(&mut r);
        let insert = take_inserts(&mut items).pop().unwrap();
        drop(insert);
        let (_, ended) = drain(&mut r);
        assert!(ended);
        assert_eq!(r.state.str(), "Done");
    });
}

#[test]
fn select_error_skips_insert() {
    with_runtime(|| {
        let mut r = new_running();
        let (mut items, _) = drain(&mut r);
        let mut select = take_select(&mut items);
        select
            .tx()
            .try_send(Err(scywr::channelwriterstate::Error::Logic))
            .ok()
            .unwrap();
        let (mut items, ended) = drain(&mut r);
        assert!(ended);
        assert!(take_inserts(&mut items).is_empty());
        assert!(r.removing);
    });
}

#[test]
fn select_dropped_skips_insert() {
    with_runtime(|| {
        let mut r = new_running();
        let (mut items, _) = drain(&mut r);
        let select = take_select(&mut items);
        drop(select);
        let (mut items, ended) = drain(&mut r);
        assert!(ended);
        assert!(take_inserts(&mut items).is_empty());
    });
}

#[test]
fn unexpected_message_emits_insert() {
    with_runtime(|| {
        let mut r = new_running();
        into_normal(&mut r);
        assert!(r.inp_push_try(rx_item(proto::CaMsgTy::Version)).is_none());
        let (mut items, ended) = drain(&mut r);
        assert!(!ended);
        assert_eq!(take_inserts(&mut items).len(), 1);
        assert_eq!(r.state.str(), "WriterStateInsert");
    });
}

#[test]
fn close_command_emits_single_insert() {
    with_runtime(|| {
        let mut r = new_running();
        into_normal(&mut r);
        r.trigger_close(ClosingReason::Command);
        let (mut items, _) = drain(&mut r);
        for _ in 0..2 {
            let ack = proto::CaMsgTy::EventAddResEmpty(proto::EventAddResEmpty {
                data_type: 6,
                sid: 7,
                subid: 1,
            });
            assert!(r.inp_push_try(rx_item(ack)).is_none());
            items.extend(drain(&mut r).0);
        }
        assert_eq!(take_inserts(&mut items).len(), 1);
        assert_eq!(r.state.str(), "WriterStateInsert");
    });
}

#[test]
fn error_after_close_does_not_duplicate_insert() {
    with_runtime(|| {
        let mut r = new_running();
        into_normal(&mut r);
        r.trigger_close(ClosingReason::Command);
        let (mut items, _) = drain(&mut r);
        let mut n = take_inserts(&mut items).len();
        r.trigger_done_error_now();
        let (mut items, _) = drain(&mut r);
        n += take_inserts(&mut items).len();
        assert_eq!(n, 1);
    });
}
