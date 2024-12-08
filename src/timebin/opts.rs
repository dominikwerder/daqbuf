use netpod::query::CacheUsage;
use query::api4::binned::BinnedQuery;

#[derive(Debug, Clone)]
pub struct BinningOptions {
    cache_usage: CacheUsage,
    allow_from_events: bool,
    allow_from_prebinned: bool,
    allow_rebin: bool,
}

impl BinningOptions {
    pub fn cache_usage(&self) -> &CacheUsage {
        &self.cache_usage
    }

    pub fn allow_from_events(&self) -> bool {
        self.allow_from_events
    }

    pub fn allow_from_prebinned(&self) -> bool {
        self.allow_from_prebinned
    }

    pub fn allow_rebin(&self) -> bool {
        self.allow_rebin
    }
}

impl From<&BinnedQuery> for BinningOptions {
    fn from(value: &BinnedQuery) -> Self {
        let cache_usage = value.cache_usage().unwrap_or(CacheUsage::Ignore);
        Self {
            cache_usage,
            allow_from_events: value.allow_from_events().unwrap_or(true),
            allow_from_prebinned: value.allow_from_prebinned().unwrap_or(true),
            allow_rebin: value.allow_rebin().unwrap_or(true),
        }
    }
}
