mod caids;
mod channel;
mod channelstateinfo;
pub(super) mod conn;
mod conncmd;
mod connevent;
mod connfut;
mod futstack;
mod progpend;
mod proto_channel;
mod scywritequeue;
mod statefut;
mod statetrans;
#[cfg(test)]
mod waker_test_a;

fn todoval<T>() -> T {
    todo!("todoval")
}
