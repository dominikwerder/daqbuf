use netpod::DtMs;

// Find the next finer bin len from the passed list.
// The list is assumed to be sorted ascending, meaning finer bin len first.
pub fn find_next_finer_bin_len(bin_len: DtMs, layers: &[DtMs]) -> Option<DtMs> {
    for l in layers.iter().rev() {
        if *l < bin_len {
            return Some(*l);
        }
    }
    None
}
