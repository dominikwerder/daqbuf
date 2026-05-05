use crate::opts::CaFind;
use futures_util::StreamExt;
use std::time::Duration;

autoerr::create_error_v1!(
    name(Error, "CaTools"),
    enum variants {
        Test,
    },
);

pub async fn find(cmd: CaFind, broadcast: String) -> Result<(), Error> {
    eprintln!("{:?}", broadcast);
    let brd = broadcast.split(",");
    let tgts = brd
        .inspect(|x| eprintln!("try to parse: [{:?}]", x))
        .map(|x| x.parse().unwrap())
        .collect();
    eprintln!("{:?}", tgts);
    let (channels_input_tx, channels_input_rx) = async_channel::bounded(10);
    let blacklist = Vec::new();
    let search_timeout = Duration::from_millis(2400);
    let batch_len_max = 1;
    let (res_tx, _res_rx) = netfetch::ca::conn2::asynchan::bounded(1, "channel-lookup-res");
    channels_input_tx.send((cmd.channel, res_tx)).await.unwrap();
    let stream =
        netfetch::ca::findioc::FindIocStream::new(channels_input_rx, tgts, blacklist, search_timeout, batch_len_max);
    let deadline = taskrun::tokio::time::sleep(Duration::from_millis(2000));
    let mut stream = Box::pin(stream.take_until(deadline));
    while let Some(e) = stream.next().await {
        eprintln!("{e:?}");
        match e {
            Ok(x) => {
                for (res, _tx) in x {
                    log::info!("{res:?}");
                }
            }
            Err(e) => {
                eprintln!("error: {e}");
            }
        }
    }
    eprintln!("done");
    Ok(())
}
