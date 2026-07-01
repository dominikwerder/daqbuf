use netpod::get_url_query_pairs;
use netpod::AppendToUrl;
use netpod::FromUrl;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use url::Url;

autoerr::create_error_v1!(
    name(Error, "ExtraOptsQuery"),
    enum variants {
        BadInt(#[from] std::num::ParseIntError),
        MissingTimerange,
        BadQuery,
        Transform(#[from] crate::transform::Error),
        Netpod(#[from] netpod::Error),
    },
);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExtraOptsQuery {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    extras: BTreeMap<String, String>,
}

impl ExtraOptsQuery {
    pub fn new() -> Self {
        Self {
            extras: BTreeMap::new(),
        }
    }

    pub fn extras(&self) -> &BTreeMap<String, String> {
        &self.extras
    }
}

impl FromUrl for ExtraOptsQuery {
    type Error = Error;

    fn from_url(url: &Url) -> Result<Self, Self::Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Self::Error> {
        let mut extras = BTreeMap::new();
        for (k, v) in pairs.iter() {
            if k.starts_with("private_") {
                extras.insert(k.clone(), v.clone());
            }
        }
        let ret = Self { extras };
        Ok(ret)
    }
}

impl AppendToUrl for ExtraOptsQuery {
    fn append_to_url(&self, url: &mut Url) {
        let mut g = url.query_pairs_mut();
        for (k, v) in self.extras.iter() {
            g.append_pair(k, v);
        }
    }
}
