use std::marker::PhantomData;

macro_rules! transitions {
    ($($from:ty => [ $( $to:ty ),* $(,)? ]; )*) => {
        $($(impl TransitionAllowed for TransitionPath<$from, $to> {})*)*
    };
}

struct ChannelCreateSent {}
struct ChannelCreated {}
struct ChannelEventAddSent {}
struct ChannelMonitoring {}

enum ChanState {
    ChannelCreateSent(ChannelCreateSent),
    ChannelMonitoring(ChannelMonitoring),
}

struct TransitionPath<A, B> {
    _p1: PhantomData<A>,
    _p2: PhantomData<B>,
}

impl<A, B> TransitionPath<A, B> {
    fn new() -> Self
    where
        Self: TransitionAllowed,
    {
        Self {
            _p1: PhantomData,
            _p2: PhantomData,
        }
    }
}

trait TransitionAllowed {}

// impl TransitionAllowed for TransitionPath<ChannelCreateSent, ChannelEventAddSent> {}

transitions! {
    ChannelCreateSent => [ChannelCreated];
    ChannelCreated => [ChannelEventAddSent, ChannelMonitoring];
}

fn transition_1(inp: ChannelCreateSent) -> ChannelCreated {
    let path = TransitionPath::<ChannelCreateSent, ChannelCreated>::new();
    todo!()
}

fn transition_2(inp: ChannelCreateSent) -> ChannelMonitoring {
    // let path = TransitionPath::<ChannelCreateSent, ChannelMonitoring>::new();
    panic!("not allowed")
}
