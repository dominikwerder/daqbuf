pub use netpod::futdbg::FutDbg;
pub use netpod::futdbg::FutDbgBox;

macro_rules! poll_a {
    ($poll:expr, $hpp:expr) => {{
        use Poll::*;
        match $poll {
            Ready(Some(x)) => {
                $hpp.mark_progress();
                match x {
                    Some(Ok(())) => {}
                    Some(Err(e)) => {
                        //
                        break Ready(Some(Err(e)));
                    }
                    None => {}
                }
            }
            Ready(None) => {}
            Pending => {
                $hpp.mark_pending();
            }
        }
    }};
}

pub(crate) use poll_a;

macro_rules! poll_b {
    ($poll:expr, $hpp:expr) => {{
        use Poll::*;
        match $poll {
            Ready(Some(x)) => {
                $hpp.mark_progress();
                match x {
                    Ok(()) => {}
                    Err(e) => {
                        break Ready(Some(Err(e)));
                    }
                }
            }
            Ready(None) => {}
            Pending => {
                $hpp.mark_pending();
            }
        }
    }};
}

pub(crate) use poll_b;

macro_rules! poll_break {
    ($poll:expr, $hpp:expr) => {{
        use Poll::*;
        match $poll {
            Ready(Some(x)) => {
                $hpp.mark_progress();
                match x {
                    Some(x) => match x {
                        Ok(x) => {
                            break Ready(Some(Ok(x)));
                        }
                        Err(e) => {
                            //
                            break Ready(Some(Err(e)));
                        }
                    },
                    None => {}
                }
            }
            Ready(None) => {}
            Pending => {
                $hpp.mark_pending();
            }
        }
    }};
}

pub(crate) use poll_break;

macro_rules! poll_stream_map_ok {
    ($poll:expr, $hpp:expr, $self2:expr, $map:expr, $streamdone:tt) => {{
        use Poll::*;
        match $poll {
            Ready(Some(x)) => {
                $hpp.mark_progress();
                match $map(x) {
                    Ok(Some(x)) => {
                        //
                        break Ready(Some(Ok(x)));
                    }
                    Ok(None) => {}
                    Err(e) => {
                        // TODO
                        $self2.state = State::Done;
                        break Ready(Some(Err(e)));
                    }
                }
            }
            Ready(None) => $streamdone,
            Pending => {
                $hpp.mark_pending();
            }
        }
    }};
}

pub(crate) use poll_stream_map_ok;

macro_rules! poll_opt_fut_map {
    ($futopt:expr, $cx:expr, $hpp:expr, $self2:expr, $cf1:ident, $map:expr, $futnone:tt) => {{
        use Poll::*;
        if let Some(fut) = $futopt.as_mut() {
            match fut.poll_unpin($cx) {
                Ready(x) => {
                    $hpp.mark_progress();
                    match $map(x) {
                        Ok(Some(x)) => {
                            $cf1 Poll::Ready(Some(Ok(x)));
                        }
                        Ok(None) => {}
                        Err(e) => {
                            // TODO
                            $self2.state = State::Done;
                            $cf1 Poll::Ready(Some(Err(e)));
                        }
                    }
                }
                Pending => {
                    $hpp.mark_pending();
                }
            }
        } else {
            $futnone
        }
    }};
}

pub(crate) use poll_opt_fut_map;
