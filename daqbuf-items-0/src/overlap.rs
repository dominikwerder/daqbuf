// TODO rename, no more deque involved
pub trait HasTimestampDeque {
    fn timestamp_min(&self) -> Option<u64>;
    fn timestamp_max(&self) -> Option<u64>;
    fn pulse_min(&self) -> Option<u64>;
    fn pulse_max(&self) -> Option<u64>;
}
