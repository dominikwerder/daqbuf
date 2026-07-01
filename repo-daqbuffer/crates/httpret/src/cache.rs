use async_channel::Receiver;
use async_channel::Sender;
use netpod::log::*;
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::SystemTime;

#[allow(unused)]
pub struct Dummy(u32);

pub enum CachePortal<V> {
    Fresh,
    Existing(Receiver<Dummy>),
    Known(V),
}

impl<V> CachePortal<V> {}

enum CacheEntry<V> {
    Waiting(SystemTime, Sender<Dummy>, Receiver<Dummy>),
    Known(SystemTime, V),
}

impl<V> CacheEntry<V> {
    fn ts(&self) -> &SystemTime {
        match self {
            CacheEntry::Waiting(ts, _, _) => ts,
            CacheEntry::Known(ts, _) => ts,
        }
    }
}

struct CacheInner<K, V> {
    map: BTreeMap<K, CacheEntry<V>>,
}

impl<K, V> CacheInner<K, V>
where
    K: Ord,
{
    const fn new() -> Self {
        Self { map: BTreeMap::new() }
    }

    fn housekeeping(&mut self) {
        if self.map.len() > 200 {
            info!("trigger housekeeping with len {}", self.map.len());
            let mut v: Vec<_> = self.map.iter().map(|(k, v)| (v.ts(), k)).collect();
            v.sort();
            let ts0 = v[v.len() / 2].0.clone();
            //let tsnow = SystemTime::now();
            //let tscut = tsnow.checked_sub(Duration::from_secs(60 * 10)).unwrap_or(tsnow);
            self.map.retain(|_k, v| v.ts() >= &ts0);
            info!("housekeeping kept len {}", self.map.len());
        }
    }
}

pub struct Cache<K, V> {
    inner: Mutex<CacheInner<K, V>>,
}

impl<K, V> Cache<K, V>
where
    K: Ord,
    V: Clone,
{
    pub const fn new() -> Self {
        Self {
            inner: Mutex::new(CacheInner::new()),
        }
    }

    pub fn housekeeping(&self) {
        let mut g = self.inner.lock().unwrap();
        g.housekeeping();
    }

    pub fn portal(&self, key: K) -> CachePortal<V> {
        use std::collections::btree_map::Entry;
        let mut g = self.inner.lock().unwrap();
        g.housekeeping();
        match g.map.entry(key) {
            Entry::Vacant(e) => {
                let (tx, rx) = async_channel::bounded(16);
                let ret = CachePortal::Fresh;
                let v = CacheEntry::Waiting(SystemTime::now(), tx, rx);
                e.insert(v);
                ret
            }
            Entry::Occupied(e) => match e.get() {
                CacheEntry::Waiting(_ts, _tx, rx) => CachePortal::Existing(rx.clone()),
                CacheEntry::Known(_ts, v) => CachePortal::Known(v.clone()),
            },
        }
    }

    pub fn set_value(&self, key: K, val: V) {
        let mut g = self.inner.lock().unwrap();
        if let Some(e) = g.map.get_mut(&key) {
            match e {
                CacheEntry::Waiting(ts, tx, _rx) => {
                    let tx = tx.clone();
                    *e = CacheEntry::Known(*ts, val);
                    tx.close();
                }
                CacheEntry::Known(_ts, _val) => {
                    error!("set_value  already known");
                }
            }
        } else {
            error!("set_value  no entry for key");
        }
    }
}
