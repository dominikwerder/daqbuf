use netpod::range::evrange::NanoRange;
use netpod::ttl::RetentionTime;
use series::msp::PrebinnedPartitioning;
use series::SeriesId;

pub trait BinWriteIndexReaderMaker {
    fn new(
        rt1: RetentionTime,
        rt2: RetentionTime,
        series: SeriesId,
        pbp: PrebinnedPartitioning,
        range: NanoRange,
        // scyqueue: ScyllaQueue,
    );
}
