use netpod::get_url_query_pairs;
use netpod::log::*;
use netpod::AppendToUrl;
use netpod::FromUrl;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use thiserror;
use url::Url;

#[derive(Debug, thiserror::Error)]
#[cstm(name = "Query")]
pub enum Error {
    ParseInt(#[from] std::num::ParseIntError),
    BadEnumAsString,
    BadBinningScheme,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum EventTransformQuery {
    EventBlobsVerbatim,
    EventBlobsUncompressed,
    ValueFull,
    ArrayPick(usize),
    // TODO should rename to scalar? dim0 will only stay a scalar.
    MinMaxAvgDev,
    PulseIdDiff,
}

impl EventTransformQuery {
    pub fn need_value_data(&self) -> bool {
        match self {
            EventTransformQuery::EventBlobsVerbatim => true,
            EventTransformQuery::EventBlobsUncompressed => true,
            EventTransformQuery::ValueFull => true,
            EventTransformQuery::ArrayPick(_) => true,
            EventTransformQuery::MinMaxAvgDev => true,
            EventTransformQuery::PulseIdDiff => false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TimeBinningTransformQuery {
    None,
    TimeWeighted,
    Unweighted,
}

impl TimeBinningTransformQuery {
    pub fn need_one_before_range(&self) -> bool {
        match self {
            TimeBinningTransformQuery::None => false,
            TimeBinningTransformQuery::TimeWeighted => true,
            TimeBinningTransformQuery::Unweighted => false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TransformQuery {
    event: EventTransformQuery,
    time_binning: TimeBinningTransformQuery,
    // #[serde(default, skip_serializing_if = "Option::is_none")]
    enum_as_string: Option<bool>,
}

impl TransformQuery {
    fn url_prefix() -> &'static str {
        "transform"
    }

    pub fn default_events() -> Self {
        Self {
            event: EventTransformQuery::ValueFull,
            time_binning: TimeBinningTransformQuery::None,
            enum_as_string: None,
        }
    }

    pub fn default_time_binned() -> Self {
        Self {
            event: EventTransformQuery::MinMaxAvgDev,
            time_binning: TimeBinningTransformQuery::TimeWeighted,
            enum_as_string: None,
        }
    }

    pub fn is_default_events(&self) -> bool {
        self == &Self::default_events()
    }

    pub fn is_default_time_binned(&self) -> bool {
        self == &Self::default_time_binned()
    }

    pub fn for_event_blobs() -> Self {
        Self {
            event: EventTransformQuery::EventBlobsVerbatim,
            time_binning: TimeBinningTransformQuery::None,
            enum_as_string: None,
        }
    }

    pub fn for_time_weighted_scalar() -> Self {
        Self {
            event: EventTransformQuery::MinMaxAvgDev,
            time_binning: TimeBinningTransformQuery::TimeWeighted,
            enum_as_string: None,
        }
    }

    pub fn for_pulse_id_diff() -> Self {
        Self {
            event: EventTransformQuery::PulseIdDiff,
            // TODO probably we want unweighted binning here.
            time_binning: TimeBinningTransformQuery::TimeWeighted,
            enum_as_string: None,
        }
    }

    pub fn is_event_blobs(&self) -> bool {
        match &self.event {
            EventTransformQuery::EventBlobsVerbatim => true,
            EventTransformQuery::EventBlobsUncompressed => {
                error!("TODO decide on uncompressed event blobs");
                panic!()
            }
            _ => false,
        }
    }

    pub fn need_value_data(&self) -> bool {
        self.event.need_value_data()
    }

    pub fn need_one_before_range(&self) -> bool {
        self.time_binning.need_one_before_range()
    }

    pub fn is_pulse_id_diff(&self) -> bool {
        match &self.event {
            EventTransformQuery::PulseIdDiff => true,
            _ => false,
        }
    }

    pub fn get_tr_event(&self) -> &EventTransformQuery {
        &self.event
    }

    pub fn get_tr_time_binning(&self) -> &TimeBinningTransformQuery {
        &self.time_binning
    }

    pub fn enum_as_string(&self) -> Option<bool> {
        self.enum_as_string.clone()
    }

    pub fn do_wasm(&self) -> Option<&str> {
        None
    }
}

impl FromUrl for TransformQuery {
    type Error = Error;

    fn from_url(url: &Url) -> Result<Self, Self::Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Self::Error> {
        let enum_as_string = if let Some(k) = pairs.get("enumAsString") {
            Some(k.parse().map_err(|_| Error::BadEnumAsString)?)
        } else {
            None
        };
        // enum_string: Ok(pairs.get("enumString")).and_then(|x| {
        //     x.map_or(Ok(None), |k| {
        //         k.parse()
        //             .map_err(|_| Error::with_public_msg_no_trace(format!("can not parse enumString: {}", k)))?
        //     })
        // })?,
        let upre = Self::url_prefix();
        let key = "binningScheme";
        if let Some(s) = pairs.get(key) {
            let ret = if s == "eventBlobs" {
                TransformQuery {
                    event: EventTransformQuery::EventBlobsVerbatim,
                    time_binning: TimeBinningTransformQuery::None,
                    enum_as_string,
                }
            } else if s == "fullValue" {
                TransformQuery {
                    event: EventTransformQuery::ValueFull,
                    time_binning: TimeBinningTransformQuery::None,
                    enum_as_string,
                }
            } else if s == "timeWeightedScalar" {
                TransformQuery {
                    event: EventTransformQuery::MinMaxAvgDev,
                    time_binning: TimeBinningTransformQuery::TimeWeighted,
                    enum_as_string,
                }
            } else if s == "unweightedScalar" {
                TransformQuery {
                    event: EventTransformQuery::ValueFull,
                    time_binning: TimeBinningTransformQuery::None,
                    enum_as_string,
                }
            } else if s == "binnedX" {
                let _u: usize = pairs.get("binnedXcount").map_or("1", |k| k).parse()?;
                warn!("TODO binnedXcount");
                TransformQuery {
                    event: EventTransformQuery::MinMaxAvgDev,
                    time_binning: TimeBinningTransformQuery::None,
                    enum_as_string,
                }
            } else if s == "pulseIdDiff" {
                TransformQuery {
                    event: EventTransformQuery::PulseIdDiff,
                    time_binning: TimeBinningTransformQuery::None,
                    enum_as_string,
                }
            } else {
                return Err(Error::BadBinningScheme);
            };
            Ok(ret)
        } else {
            // TODO add option to pick from array.
            let _pick = pairs
                .get(&format!("{}ArrayPick", upre))
                .map(|x| match x.parse::<usize>() {
                    Ok(n) => Some(n),
                    Err(_) => None,
                })
                .unwrap_or(None);
            let ret = TransformQuery {
                event: EventTransformQuery::ValueFull,
                time_binning: TimeBinningTransformQuery::None,
                enum_as_string,
            };
            Ok(ret)
        }
    }
}

impl AppendToUrl for TransformQuery {
    fn append_to_url(&self, url: &mut Url) {
        let mut g = url.query_pairs_mut();
        if false {
            let upre = Self::url_prefix();
            if let Some(x) = &Some(123) {
                g.append_pair(&format!("{}ArrayPick", upre), &format!("{}", x));
            }
        }
        let key = "binningScheme";
        match &self.event {
            EventTransformQuery::EventBlobsVerbatim => {
                g.append_pair(key, &format!("{}", "eventBlobs"));
            }
            EventTransformQuery::EventBlobsUncompressed => {
                // TODO
                g.append_pair(key, &format!("{}", "eventBlobs"));
            }
            EventTransformQuery::ValueFull => {
                g.append_pair(key, &format!("{}", "fullValue"));
            }
            EventTransformQuery::ArrayPick(_) => {
                // TODO
                g.append_pair(key, &format!("{}", "fullValue"));
            }
            EventTransformQuery::MinMaxAvgDev => {
                g.append_pair(key, &format!("{}", "timeWeightedScalar"));
            }
            EventTransformQuery::PulseIdDiff => {
                g.append_pair(key, &format!("{}", "pulseIdDiff"));
            }
        }
        if let Some(x) = self.enum_as_string {
            if x {
                g.append_pair("enumAsString", "true");
            }
        }
    }
}
