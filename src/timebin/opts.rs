use netpod::query::CacheUsage;
use query::api4::binned::BinnedQuery;

#[derive(Debug, Clone)]
pub struct BinningOptions {
    cache_usage: CacheUsage,
    allow_from_events: bool,
    allow_from_prebinned: bool,
    allow_rebin: bool,
    pbd_enable: bool,
    pbd_rts_pbp_block: Vec<Vec<u8>>,
    pbd_evs: bool,
}

impl BinningOptions {
    pub fn default() -> Self {
        Self {
            cache_usage: CacheUsage::default(),
            allow_from_events: true,
            allow_from_prebinned: true,
            allow_rebin: true,
            pbd_enable: false,
            pbd_rts_pbp_block: Vec::new(),
            pbd_evs: false,
        }
    }

    pub fn testing_no_events() -> Self {
        Self {
            cache_usage: CacheUsage::default(),
            allow_from_events: false,
            allow_from_prebinned: true,
            allow_rebin: true,
            pbd_enable: false,
            pbd_rts_pbp_block: Vec::new(),
            pbd_evs: false,
        }
    }

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

    pub fn pbd_enable(&self) -> bool {
        self.pbd_enable.clone()
    }

    pub fn pbd_rts_pbp_block(&self) -> Vec<Vec<u8>> {
        self.pbd_rts_pbp_block.clone()
    }

    pub fn pbd_evs(&self) -> bool {
        self.pbd_evs.clone()
    }
}

impl From<&BinnedQuery> for BinningOptions {
    fn from(value: &BinnedQuery) -> Self {
        let cache_usage = value.cache_usage().unwrap_or(CacheUsage::default());
        Self {
            cache_usage,
            allow_from_events: value.allow_from_events().unwrap_or(true),
            allow_from_prebinned: value.allow_from_prebinned().unwrap_or(true),
            allow_rebin: value.allow_rebin().unwrap_or(true),
            pbd_enable: value.pbd_enable().unwrap_or(false),
            pbd_rts_pbp_block: value.pbd_rts_pbp_block().unwrap_or(Vec::new()),
            pbd_evs: value.pbd_evs().unwrap_or(false),
        }
    }
}
