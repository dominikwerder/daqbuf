use netpod::get_url_query_pairs;
use netpod::AppendToUrl;
use netpod::FromUrl;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use url::Url;

autoerr::create_error_v1!(
    name(Error, "ScyllaOptsQuery"),
    enum variants {
        BadInt(#[from] std::num::ParseIntError),
        MissingTimerange,
        BadQuery,
        Transform(#[from] crate::transform::Error),
        Netpod(#[from] netpod::Error),
    },
);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScyllaOptsQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    msp_cache_bypass: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    msp_order_desc_read_all_asc: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    order_asc_cache_bypass: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    order_desc_cache_bypass: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    order_desc_read_all_asc: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    evs_lsp_order_desc_read_all_asc_cache_bypass: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    evs_val_order_asc_cache_bypass: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    bins_fwd_cache_bypass: Option<bool>,
}

impl ScyllaOptsQuery {
    pub fn new() -> Self {
        Self {
            msp_cache_bypass: None,
            msp_order_desc_read_all_asc: None,
            order_asc_cache_bypass: None,
            order_desc_cache_bypass: None,
            order_desc_read_all_asc: None,
            evs_lsp_order_desc_read_all_asc_cache_bypass: None,
            evs_val_order_asc_cache_bypass: None,
            bins_fwd_cache_bypass: None,
        }
    }

    fn cache_bypass_default(&self) -> bool {
        true
    }

    fn order_desc_read_all_asc_default(&self) -> bool {
        true
    }

    pub fn msp_cache_bypass(&self) -> bool {
        self.msp_cache_bypass.unwrap_or(self.cache_bypass_default())
    }

    pub fn msp_order_desc_read_all_asc(&self) -> bool {
        self.msp_order_desc_read_all_asc
            .unwrap_or(self.order_desc_read_all_asc_default())
    }

    pub fn order_asc_cache_bypass(&self) -> bool {
        self.order_asc_cache_bypass
            .unwrap_or(self.cache_bypass_default())
    }

    pub fn order_desc_cache_bypass(&self) -> bool {
        self.order_desc_cache_bypass
            .unwrap_or(self.cache_bypass_default())
    }

    pub fn order_desc_read_all_asc(&self) -> bool {
        self.order_desc_read_all_asc
            .unwrap_or(self.order_desc_read_all_asc_default())
    }

    pub fn evs_lsp_order_desc_read_all_asc_cache_bypass(&self) -> bool {
        self.evs_lsp_order_desc_read_all_asc_cache_bypass
            .unwrap_or(self.cache_bypass_default())
    }

    pub fn evs_val_order_asc_cache_bypass(&self) -> bool {
        self.evs_val_order_asc_cache_bypass
            .unwrap_or(self.cache_bypass_default())
    }

    pub fn bins_fwd_cache_bypass(&self) -> bool {
        self.bins_fwd_cache_bypass
            .unwrap_or(self.cache_bypass_default())
    }
}

impl FromUrl for ScyllaOptsQuery {
    type Error = Error;

    fn from_url(url: &Url) -> Result<Self, Self::Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Self::Error> {
        let mut dummy = None;
        let mut msp_cache_bypass = None;
        let mut msp_order_desc_read_all_asc = None;
        let mut order_asc_cache_bypass = None;
        let mut order_desc_cache_bypass = None;
        let mut order_desc_read_all_asc = None;
        let mut evs_lsp_order_desc_read_all_asc_cache_bypass = None;
        let mut evs_val_order_asc_cache_bypass = None;
        let mut bins_fwd_cache_bypass = None;
        pairs.get("scyllaOptsFlags").map(|s| {
            s.split(",").for_each(|x| {
                let mut it = x.split("=");
                if let Some(x) = it.next() {
                    let f = if x == "" {
                        &mut dummy
                    } else if x == "msp_cache_bypass" {
                        &mut msp_cache_bypass
                    } else if x == "msp_order_desc_read_all_asc" {
                        &mut msp_order_desc_read_all_asc
                    } else if x == "order_asc_cache_bypass" {
                        &mut order_asc_cache_bypass
                    } else if x == "order_desc_cache_bypass" {
                        &mut order_desc_cache_bypass
                    } else if x == "order_desc_read_all_asc" {
                        &mut order_desc_read_all_asc
                    } else if x == "evs_lsp_order_desc_read_all_asc_cache_bypass" {
                        &mut evs_lsp_order_desc_read_all_asc_cache_bypass
                    } else if x == "evs_val_order_asc_cache_bypass" {
                        &mut evs_val_order_asc_cache_bypass
                    } else if x == "bins_fwd_cache_bypass" {
                        &mut bins_fwd_cache_bypass
                    } else {
                        &mut dummy
                    };
                    if let Some(v) = it.next() {
                        if let Ok(v) = v.parse::<bool>() {
                            *f = Some(v);
                        }
                    }
                }
            });
        });
        let _ = dummy;
        let ret = Self {
            msp_cache_bypass,
            msp_order_desc_read_all_asc,
            order_asc_cache_bypass,
            order_desc_cache_bypass,
            order_desc_read_all_asc,
            evs_lsp_order_desc_read_all_asc_cache_bypass,
            evs_val_order_asc_cache_bypass,
            bins_fwd_cache_bypass,
        };
        Ok(ret)
    }
}

impl AppendToUrl for ScyllaOptsQuery {
    fn append_to_url(&self, url: &mut Url) {
        let mut flags = Vec::new();
        if let Some(v) = self.msp_cache_bypass {
            flags.push(format!("msp_cache_bypass={v}"));
        }
        if let Some(v) = self.msp_order_desc_read_all_asc {
            flags.push(format!("msp_order_desc_read_all_asc={v}"));
        }
        if let Some(v) = self.order_asc_cache_bypass {
            flags.push(format!("order_asc_cache_bypass={v}"));
        }
        if let Some(v) = self.order_desc_cache_bypass {
            flags.push(format!("order_desc_cache_bypass={v}"));
        }
        if let Some(v) = self.order_desc_read_all_asc {
            flags.push(format!("order_desc_read_all_asc={v}"));
        }
        if let Some(v) = self.evs_lsp_order_desc_read_all_asc_cache_bypass {
            flags.push(format!("evs_lsp_order_desc_read_all_asc_cache_bypass={v}"));
        }
        if let Some(v) = self.evs_val_order_asc_cache_bypass {
            flags.push(format!("evs_val_order_asc_cache_bypass={v}"));
        }
        if let Some(v) = self.bins_fwd_cache_bypass {
            flags.push(format!("bins_fwd_cache_bypass={v}"));
        }
        let mut g = url.query_pairs_mut();
        if flags.len() != 0 {
            g.append_pair("scyllaOptsFlags", &flags.join(","));
        }
    }
}
