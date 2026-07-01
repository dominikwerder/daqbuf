use crate::conn::events_conn_handler;
use crate::conn::Frame1Parts;
use daqbuf_err as err;
use err::Error;
use futures_util::StreamExt;
use items_0::streamitem::sitem_data;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::streamitem::ERROR_FRAME_TYPE_ID;
use items_0::streamitem::ITEMS_2_CHANNEL_EVENTS_FRAME_TYPE_ID;
use items_0::streamitem::LOG_FRAME_TYPE_ID;
use items_0::streamitem::STATS_FRAME_TYPE_ID;
use items_2::channelevents::ChannelEvents;
use items_2::framable::EventQueryJsonStringFrame;
use items_2::framable::Framable;
use items_2::frame::decode_frame;
use netpod::range::evrange::NanoRange;
use netpod::timeunits::DAY;
use netpod::timeunits::SEC;
use netpod::ByteOrder;
use netpod::Cluster;
use netpod::DtNano;
use netpod::Node;
use netpod::NodeConfig;
use netpod::NodeConfigCached;
use netpod::ScalarType;
use netpod::SfChFetchInfo;
use netpod::SfDatabuffer;
use netpod::Shape;
use query::api4::events::EventsSubQuery;
use query::api4::events::EventsSubQuerySelect;
use query::api4::events::EventsSubQuerySettings;
use query::transform::TransformQuery;
use streamio::tcpreadasbytes::TcpReadAsBytes;
use streams::frames::inmem::InMemoryFrameStream;
use taskrun::tokio;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

// TODO unify with Cluster::test_00()
const TEST_BACKEND: &str = "testbackend-00";

#[test]
fn raw_data_00() {
    let fut = async {
        let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut con = TcpStream::connect(lis.local_addr().unwrap()).await.unwrap();
        let (client, addr) = lis.accept().await.unwrap();
        let cfg = NodeConfigCached {
            node_config: NodeConfig {
                name: "node_name_dummy".into(),
                cluster: Cluster::test_00(),
            },
            node: Node {
                host: "empty".into(),
                listen: None,
                port: 9090,
                port_raw: 9090,
                sf_databuffer: Some(SfDatabuffer {
                    data_base_path: "/home/dominik/daqbuffer-testdata/databuffer/node00".into(),
                    ksprefix: "ks".into(),
                    splits: None,
                }),
                archiver_appliance: None,
                channel_archiver: None,
                prometheus_api_bind: None,
            },
            ix: 0,
        };
        let range = NanoRange {
            beg: SEC,
            end: SEC * 10,
        };
        let fetch_info = SfChFetchInfo::new(
            TEST_BACKEND,
            "scalar-i32",
            2,
            DtNano::from_ns(DAY),
            ByteOrder::Big,
            ScalarType::I32,
            Shape::Scalar,
        );
        let select =
            EventsSubQuerySelect::new(fetch_info.into(), range.into(), false, TransformQuery::default_events());
        let settings = EventsSubQuerySettings::default();
        let log_level = String::new();
        let qu = EventsSubQuery::from_parts(select, settings, "dummy".into(), log_level);
        let frame1 = Frame1Parts::new(qu.clone());
        let query = EventQueryJsonStringFrame(serde_json::to_string(&frame1).unwrap());
        let frame = sitem_data(query).make_frame_dyn().map_err(Error::from_string)?;
        let scyqueue = err::todoval();
        let jh = taskrun::spawn(events_conn_handler(client, addr, scyqueue, cfg));
        con.write_all(&frame).await.unwrap();
        eprintln!("written");
        con.shutdown().await.unwrap();
        eprintln!("shut down");
        // TODO use?
        let (netin, _netout) = con.into_split();
        let mut frames = InMemoryFrameStream::new(TcpReadAsBytes::new(netin), qu.inmem_bufcap());
        while let Some(frame) = frames.next().await {
            match frame {
                Ok(x) => match x {
                    StreamItem::DataItem(RangeCompletableItem::Data(k)) => {
                        eprintln!("{k:?}");
                        if k.tyid() == ITEMS_2_CHANNEL_EVENTS_FRAME_TYPE_ID {
                        } else if k.tyid() == ERROR_FRAME_TYPE_ID {
                        } else if k.tyid() == LOG_FRAME_TYPE_ID {
                        } else if k.tyid() == STATS_FRAME_TYPE_ID {
                        } else {
                            panic!("unexpected frame type id {:x}", k.tyid());
                        }
                        let item: Sitemty<ChannelEvents> = decode_frame(&k).unwrap();
                        eprintln!("decoded: {:?}", item);
                    }
                    StreamItem::DataItem(RangeCompletableItem::RangeComplete) => {
                        eprintln!("decoded: RangeComplete");
                        todo!()
                    }
                    StreamItem::Log(_) => todo!(),
                    StreamItem::Stats(_) => todo!(),
                },
                Err(e) => {
                    panic!("{e:?}");
                }
            }
        }
        jh.await.unwrap().unwrap();
        Ok::<_, Error>(())
    };
    taskrun::run(fut).unwrap();
}
