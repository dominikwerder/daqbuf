use crate::eventchunker::EventChunkerConf;
use crate::eventchunkermultifile::EventChunkerMultifile;
use crate::AggQuerySingleChannel;
use crate::SfDbChConf;
use daqbuf_err as err;
use err::Error;
use netpod::range::evrange::NanoRange;
use netpod::test_data_base_path_databuffer;
use netpod::timeunits::*;
use netpod::ByteOrder;
use netpod::ByteSize;
use netpod::DiskIoTune;
use netpod::DtNano;
use netpod::Node;
use netpod::ScalarType;
use netpod::SfChFetchInfo;
use netpod::SfDatabuffer;
use netpod::SfDbChannel;
use netpod::Shape;

pub fn make_test_node(id: u32) -> Node {
    Node {
        host: "localhost".into(),
        listen: None,
        port: 8800 + id as u16,
        port_raw: 8800 + id as u16 + 100,
        // TODO use a common function to supply the tmp path.
        sf_databuffer: Some(SfDatabuffer {
            data_base_path: test_data_base_path_databuffer().join(format!("node{:02}", id)),
            ksprefix: "ks".into(),
            splits: None,
        }),
        archiver_appliance: None,
        channel_archiver: None,
        prometheus_api_bind: None,
    }
}

#[test]
fn agg_x_dim_0() {
    taskrun::run(async {
        agg_x_dim_0_inner().await;
        Ok::<_, Error>(())
    })
    .unwrap();
}

async fn agg_x_dim_0_inner() {
    let node = make_test_node(0);
    let query = AggQuerySingleChannel {
        channel_config: SfDbChConf {
            channel: SfDbChannel::from_name("sf-databuffer", "S10BC01-DBAM070:EOM1_T1"),
            keyspace: 2,
            time_bin_size: DtNano::from_ns(DAY),
            array: false,
            shape: Shape::Scalar,
            scalar_type: ScalarType::F64,
            byte_order: ByteOrder::Big,
            compression: true,
        },
        timebin: 18723,
        tb_file_count: 1,
        buffer_size: 1024 * 4,
    };
    let fetch_info = SfChFetchInfo::new(
        "sf-databuffer",
        "S10BC01-DBAM070:EOM1_T1",
        2,
        DtNano::from_ns(DAY),
        ByteOrder::Big,
        ScalarType::F64,
        Shape::Scalar,
    );
    let _bin_count = 20;
    let ts1 = query.timebin as u64 * query.channel_config.time_bin_size.ns();
    let ts2 = ts1 + HOUR * 24;
    let range = NanoRange { beg: ts1, end: ts2 };
    let event_chunker_conf = EventChunkerConf::new(ByteSize::from_kb(1024));
    // TODO let upstream already provide DiskIoTune:
    let mut disk_io_tune = DiskIoTune::default_for_testing();
    disk_io_tune.read_buffer_len = query.buffer_size as usize;
    let fut1 = EventChunkerMultifile::new(
        range.clone(),
        fetch_info,
        node.clone(),
        0,
        disk_io_tune,
        event_chunker_conf,
        true,
        // TODO
        32,
        netpod::ReqCtx::for_test().into(),
    );
    let _ = fut1;
    // TODO add the binning and expectation and await the result.
}

#[test]
fn agg_x_dim_1() {
    taskrun::run(async {
        agg_x_dim_1_inner().await;
        Ok::<_, Error>(())
    })
    .unwrap();
}

async fn agg_x_dim_1_inner() {
    // sf-databuffer
    // /data/sf-databuffer/daq_swissfel/daq_swissfel_3/byTime/S10BC01-DBAM070\:BAM_CH1_NORM/*
    // S10BC01-DBAM070:BAM_CH1_NORM
    let node = make_test_node(0);
    let query = AggQuerySingleChannel {
        channel_config: SfDbChConf {
            channel: SfDbChannel::from_name("ks", "wave1"),
            keyspace: 3,
            time_bin_size: DtNano::from_ns(DAY),
            array: true,
            shape: Shape::Wave(1024),
            scalar_type: ScalarType::F64,
            byte_order: ByteOrder::Big,
            compression: true,
        },
        timebin: 0,
        tb_file_count: 1,
        buffer_size: 17,
    };
    let fetch_info = SfChFetchInfo::new(
        "ks",
        "wave1",
        2,
        DtNano::from_ns(DAY),
        ByteOrder::Big,
        ScalarType::F64,
        Shape::Scalar,
    );
    let _bin_count = 10;
    let ts1 = query.timebin as u64 * query.channel_config.time_bin_size.ns();
    let ts2 = ts1 + HOUR * 24;
    let range = NanoRange { beg: ts1, end: ts2 };
    let event_chunker_conf = EventChunkerConf::new(ByteSize::from_kb(1024));
    // TODO let upstream already provide DiskIoTune:
    let mut disk_io_tune = DiskIoTune::default_for_testing();
    disk_io_tune.read_buffer_len = query.buffer_size as usize;
    let fut1 = super::eventchunkermultifile::EventChunkerMultifile::new(
        range.clone(),
        fetch_info,
        node.clone(),
        0,
        disk_io_tune,
        event_chunker_conf,
        true,
        // TODO
        32,
        netpod::ReqCtx::for_test().into(),
    );
    let _ = fut1;
    // TODO add the binning and expectation and await the result.
}
