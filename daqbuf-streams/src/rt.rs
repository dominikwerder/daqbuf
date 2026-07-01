use futures_util::Future;

pub fn run_test<F>(fut: F) -> <F as Future>::Output
where
    F: Future,
{
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(fut)
}
