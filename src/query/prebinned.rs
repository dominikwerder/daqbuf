use super::agg_kind_from_binning_scheme;
use super::binning_scheme_append_to_url;
use super::CacheUsage;
use crate::AggKind;
use crate::AppendToUrl;
use crate::ByteSize;
use crate::FromUrl;
use crate::NetpodError;
use crate::PreBinnedPatchCoordEnum;
use crate::ScalarType;
use crate::SfDbChannel;
use crate::Shape;
use std::collections::BTreeMap;
use url::Url;

#[derive(Clone, Debug)]
pub struct PreBinnedQuery {
    patch: PreBinnedPatchCoordEnum,
    channel: SfDbChannel,
    scalar_type: ScalarType,
    shape: Shape,
    agg_kind: Option<AggKind>,
    cache_usage: Option<CacheUsage>,
    buf_len_disk_io: Option<usize>,
    disk_stats_every: Option<ByteSize>,
}

impl PreBinnedQuery {
    pub fn new(
        patch: PreBinnedPatchCoordEnum,
        channel: SfDbChannel,
        scalar_type: ScalarType,
        shape: Shape,
        agg_kind: Option<AggKind>,
        cache_usage: Option<CacheUsage>,
        buf_len_disk_io: Option<usize>,
        disk_stats_every: Option<ByteSize>,
    ) -> Self {
        Self {
            patch,
            channel,
            scalar_type,
            shape,
            agg_kind,
            cache_usage,
            buf_len_disk_io,
            disk_stats_every,
        }
    }

    pub fn from_url(url: &Url) -> Result<Self, NetpodError> {
        let mut pairs = BTreeMap::new();
        for (j, k) in url.query_pairs() {
            pairs.insert(j.to_string(), k.to_string());
        }
        let pairs = pairs;
        let scalar_type = pairs
            .get("scalarType")
            .ok_or_else(|| NetpodError::MissingScalarType)
            .map(|x| ScalarType::from_url_str(&x))??;
        let shape = pairs
            .get("shape")
            .ok_or_else(|| NetpodError::MissingShape)
            .map(|x| Shape::from_url_str(&x))??;
        let ret = Self {
            patch: PreBinnedPatchCoordEnum::from_pairs(&pairs)?,
            channel: SfDbChannel::from_pairs(&pairs)?,
            scalar_type,
            shape,
            agg_kind: agg_kind_from_binning_scheme(&pairs)?,
            cache_usage: CacheUsage::from_pairs(&pairs)?,
            buf_len_disk_io: pairs
                .get("bufLenDiskIo")
                .map_or(Ok(None), |k| k.parse().map(|k| Some(k)))?,
            disk_stats_every: pairs
                .get("diskStatsEveryKb")
                .map(|k| k.parse().ok())
                .unwrap_or(None)
                .map(ByteSize::from_kb),
        };
        Ok(ret)
    }

    pub fn patch(&self) -> &PreBinnedPatchCoordEnum {
        &self.patch
    }

    pub fn channel(&self) -> &SfDbChannel {
        &self.channel
    }

    pub fn scalar_type(&self) -> &ScalarType {
        &self.scalar_type
    }

    pub fn shape(&self) -> &Shape {
        &self.shape
    }

    pub fn agg_kind(&self) -> &Option<AggKind> {
        &self.agg_kind
    }

    pub fn disk_stats_every(&self) -> ByteSize {
        match &self.disk_stats_every {
            Some(x) => x.clone(),
            None => ByteSize(1024 * 1024 * 4),
        }
    }

    pub fn cache_usage(&self) -> CacheUsage {
        self.cache_usage.as_ref().map_or(CacheUsage::Use, |x| x.clone())
    }

    pub fn buf_len_disk_io(&self) -> usize {
        self.buf_len_disk_io.unwrap_or(1024 * 8)
    }
}

impl AppendToUrl for PreBinnedQuery {
    fn append_to_url(&self, url: &mut Url) {
        if false {
            panic!("remove, not in use");
        }
        self.patch.append_to_url(url);
        self.channel.append_to_url(url);
        self.shape.append_to_url(url);
        self.scalar_type.append_to_url(url);
        if let Some(x) = &self.agg_kind {
            binning_scheme_append_to_url(x, url);
        }
        let mut g = url.query_pairs_mut();
        // TODO add also impl AppendToUrl for these if applicable:
        if let Some(x) = &self.cache_usage {
            g.append_pair("cacheUsage", &x.query_param_value());
        }
        if let Some(x) = self.buf_len_disk_io {
            g.append_pair("bufLenDiskIo", &format!("{}", x));
        }
        if let Some(x) = &self.disk_stats_every {
            g.append_pair("diskStatsEveryKb", &format!("{}", x.bytes() / 1024));
        }
    }
}
