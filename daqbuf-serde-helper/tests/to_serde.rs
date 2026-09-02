use daqbuf_serde_helper as serde_helper;
use serde_helper::ToSerde;
use std::collections::VecDeque;
use std::time::Duration;
use std::time::Instant;

/// Stands in for the non-serializable payloads in the real state machines
/// (`FutDbg`, `Waker`, channel handles).
struct Opaque;

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
struct Sid(u32);

/// A leaf component, nested into the parent below.
///
/// Adjacent tagging (`tag` + `content`) rather than internal tagging: once a variant's
/// payload is a scalar (a skipped field can leave a single `Duration` behind), serde
/// refuses to serialize an internally tagged newtype variant.
#[derive(ToSerde)]
#[to_serde(crate = serde_helper, vis = "pub", serde(tag = "type", content = "c"))]
enum Inner {
    Waiting(#[to_serde(elapsed_ms)] Instant, #[to_serde(skip)] Opaque),
    Done,
}

#[derive(ToSerde)]
#[to_serde(crate = serde_helper, serde(tag = "ty", content = "co"))]
enum Outer {
    Init(#[to_serde(elapsed_ms)] Instant),
    Sending {
        #[to_serde(elapsed_ms)]
        ts: Instant,
        #[to_serde(len)]
        buf: VecDeque<u32>,
        #[to_serde(skip)]
        fut: Opaque,
        sid: Sid,
    },
    Running(#[to_serde(elapsed_ms)] Instant, #[to_serde(nest)] Inner),
    #[to_serde(extra(note: &'static str = "terminal"))]
    Done,
}

fn ms(v: &serde_json::Value, p: &str) -> u64 {
    v.pointer(p)
        .unwrap_or_else(|| panic!("no {p} in {v}"))
        .as_u64()
        .unwrap_or_else(|| panic!("{p} not a number in {v}"))
}

#[test]
fn enum_tuple_variant_elapsed() {
    let st = Outer::Init(Instant::now() - Duration::from_millis(1500));
    let v = serde_json::to_value(st.to_serde()).unwrap();
    assert_eq!(v["ty"], "Init");
    let age = ms(&v, "/co");
    assert!((1500..1600).contains(&age), "age was {age}");
}

#[test]
fn enum_named_variant_skip_len_and_clone() {
    let st = Outer::Sending {
        ts: Instant::now(),
        buf: VecDeque::with_capacity(64),
        fut: Opaque,
        sid: Sid(7),
    };
    let v = serde_json::to_value(st.to_serde()).unwrap();
    assert_eq!(v["ty"], "Sending");
    assert_eq!(v["co"]["buf"]["len"], 0);
    assert_eq!(v["co"]["buf"]["cap"], 64);
    assert_eq!(v["co"]["sid"], 7);
    // the opaque payload must not appear at all
    assert!(v["co"].get("fut").is_none(), "fut leaked into {v}");
}

#[test]
fn nested_component_recurses() {
    let st = Outer::Running(
        Instant::now(),
        Inner::Waiting(Instant::now() - Duration::from_millis(300), Opaque),
    );
    let v = serde_json::to_value(st.to_serde()).unwrap();
    assert_eq!(v["ty"], "Running");
    assert_eq!(v["co"][1]["type"], "Waiting");
    let age = ms(&v, "/co/1/c");
    assert!((300..400).contains(&age), "inner age was {age}");
}

#[test]
fn variant_extra_field() {
    let v = serde_json::to_value(Outer::Done.to_serde()).unwrap();
    assert_eq!(v["ty"], "Done");
    // a single extra leaves a newtype variant, which serde emits unwrapped
    assert_eq!(v["co"], "terminal");
}

// ---- structs, including derived fields computed only on snapshot ----

struct Counters {
    events: u64,
}

#[derive(ToSerde)]
#[to_serde(crate = serde_helper, vis = "pub")]
#[to_serde(extra(fill: f32 = self.fill()))]
#[to_serde(extra(events_per_s: f64 = self.rate()))]
struct Handler {
    #[to_serde(nest)]
    state: Inner,
    sid: Sid,
    #[to_serde(len)]
    inp: VecDeque<u32>,
    #[to_serde(skip)]
    counters: Counters,
    #[to_serde(elapsed_ms)]
    ts_start: Instant,
}

impl Handler {
    fn fill(&self) -> f32 {
        self.inp.len() as f32 / self.inp.capacity() as f32
    }

    /// Stands in for a value that is costly enough that it must not be kept up to date
    /// eagerly — it is only computed when a snapshot is taken.
    fn rate(&self) -> f64 {
        let secs = self.ts_start.elapsed().as_secs_f64();
        if secs > 0.0 {
            self.counters.events as f64 / secs
        } else {
            0.0
        }
    }
}

#[test]
fn struct_with_derived_fields() {
    let mut inp = VecDeque::with_capacity(4);
    inp.push_back(1);
    let h = Handler {
        state: Inner::Done,
        sid: Sid(3),
        inp,
        counters: Counters { events: 100 },
        ts_start: Instant::now() - Duration::from_millis(1000),
    };
    let v = serde_json::to_value(h.to_serde()).unwrap();
    assert_eq!(v["state"]["type"], "Done");
    assert_eq!(v["sid"], 3);
    assert_eq!(v["inp"]["len"], 1);
    assert_eq!(v["inp"]["cap"], 4);
    assert!(v.get("counters").is_none(), "counters leaked into {v}");
    assert_eq!(v["fill"], 0.25);
    let rate = v["events_per_s"].as_f64().unwrap();
    assert!((90.0..110.0).contains(&rate), "rate was {rate}");
}

// ---- `elapsed` uses the human duration helper, `with` is the escape hatch ----

fn opaque_repr(_x: &Opaque) -> &'static str {
    "opaque"
}

#[derive(ToSerde)]
#[to_serde(crate = serde_helper)]
struct Misc {
    #[to_serde(elapsed)]
    ts: Instant,
    #[to_serde(with = opaque_repr, ty = &'static str)]
    payload: Opaque,
}

#[test]
fn elapsed_is_human_and_with_is_applied() {
    let m = Misc {
        ts: Instant::now() - Duration::from_millis(2500),
        payload: Opaque,
    };
    let v = serde_json::to_value(m.to_serde()).unwrap();
    assert_eq!(v["ts"], "2s500ms");
    assert_eq!(v["payload"], "opaque");
}

// ---- dwell-warn-score: constant per-variant, and runtime-context-supplied ----

#[derive(ToSerde)]
#[to_serde(crate = serde_helper, serde(tag = "ty", content = "co"), dwell_ctx = Duration)]
enum DwellState {
    DoNothing,
    #[to_serde(dwell = *ctx)]
    Idle(#[to_serde(skip)] Opaque),
    #[to_serde(dwell_ms = 3000)]
    WaitRes(#[to_serde(skip)] Opaque),
    Closing1,
}

#[test]
fn dwell_typical_constant_and_ctx() {
    let interval = Duration::from_millis(2000);
    assert_eq!(DwellState::DoNothing.dwell_typical(&interval), None);
    assert_eq!(DwellState::Idle(Opaque).dwell_typical(&interval), Some(interval));
    assert_eq!(
        DwellState::WaitRes(Opaque).dwell_typical(&interval),
        Some(Duration::from_millis(3000))
    );
    assert_eq!(DwellState::Closing1.dwell_typical(&interval), None);
}

/// Mirrors the split-timestamp shape used across the ca/conn2 state machines: the entry
/// `Instant` lives on the parent, `DwellState` lives alongside it, and the typical dwell for
/// `Idle` is a runtime parameter (`interval`) rather than a compile-time constant.
#[derive(ToSerde)]
#[to_serde(crate = serde_helper)]
struct DwellHolder {
    #[to_serde(nest)]
    state: DwellState,
    #[to_serde(elapsed_ms, dwell = self.state.dwell_typical(&self.interval))]
    state_dt: Instant,
    #[to_serde(serde(with = "serde_helper::serde_Duration_human"))]
    interval: Duration,
}

#[test]
fn dwell_score_present_for_variant_with_dwell() {
    let h = DwellHolder {
        state: DwellState::Idle(Opaque),
        state_dt: Instant::now() - Duration::from_millis(3000),
        interval: Duration::from_millis(2000),
    };
    let v = serde_json::to_value(h.to_serde()).unwrap();
    let score = v["dwell_score"].as_f64().unwrap();
    // 3000ms elapsed / 2000ms typical = 1.5
    assert!((1.4..1.6).contains(&score), "score was {score}");
}

#[test]
fn dwell_score_absent_for_variant_without_dwell() {
    let h = DwellHolder {
        state: DwellState::DoNothing,
        state_dt: Instant::now(),
        interval: Duration::from_millis(2000),
    };
    let v = serde_json::to_value(h.to_serde()).unwrap();
    assert!(v["dwell_score"].is_null(), "{v}");
}

#[test]
fn dwell_score_uses_hardcoded_constant() {
    let h = DwellHolder {
        state: DwellState::WaitRes(Opaque),
        state_dt: Instant::now() - Duration::from_millis(1500),
        interval: Duration::from_millis(2000), // irrelevant for the constant-dwell variant
    };
    let v = serde_json::to_value(h.to_serde()).unwrap();
    let score = v["dwell_score"].as_f64().unwrap();
    // 1500ms elapsed / 3000ms constant = 0.5
    assert!((0.4..0.6).contains(&score), "score was {score}");
}

#[test]
fn dwell_score_is_f32_rounded_to_1e3() {
    let h = DwellHolder {
        state: DwellState::WaitRes(Opaque),
        state_dt: Instant::now() - Duration::from_millis(777),
        interval: Duration::from_millis(2000), // irrelevant for the constant-dwell variant
    };
    let v = serde_json::to_value(h.to_serde()).unwrap();
    // serde_json represents f32 losslessly as f64, so the value read back must already sit
    // on a multiple of 0.001 if the derive rounded it as an f32 before serializing.
    let score = v["dwell_score"].as_f64().unwrap() as f32;
    let scaled = score * 1000.0;
    assert!(
        (scaled - scaled.round()).abs() < 1e-3,
        "dwell_score {score} is not rounded to 1e-3"
    );
}

/// Embedded pattern (create.rs's SeriesIdRecv): elapsed and dwell live in the same variant,
/// no cross-referencing needed.
#[derive(ToSerde)]
#[to_serde(crate = serde_helper, serde(tag = "ty", content = "co"))]
enum EmbeddedDwellState {
    Waiting(
        #[to_serde(elapsed_ms, dwell_ms = 1000)] Instant,
        #[to_serde(skip)] Opaque,
    ),
}

#[test]
fn embedded_dwell_score_sibling_in_same_variant() {
    let st = EmbeddedDwellState::Waiting(Instant::now() - Duration::from_millis(800), Opaque);
    let v = serde_json::to_value(st.to_serde()).unwrap();
    assert_eq!(v["ty"], "Waiting");
    let score = v["co"][1].as_f64().unwrap();
    assert!((0.7..0.9).contains(&score), "score was {score}");
}
