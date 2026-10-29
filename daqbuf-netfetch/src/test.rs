use items_2::binning::container_events::ContainerEvents;
use items_2::binning::container_events::EventValueType;
use netpod::EnumVariant;
use netpod::TsNano;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;

fn check<T>(x: &T, golden_hex: &str)
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    let golden = hex::decode(golden_hex).unwrap();
    let enc = cbor2::to_vec(x).unwrap();
    assert_eq!(hex::encode(&enc), golden_hex, "encoding differs from golden");
    let back: T = cbor2::from_slice(&golden).unwrap();
    assert_eq!(
        hex::encode(cbor2::to_vec(&back).unwrap()),
        golden_hex,
        "roundtrip differs"
    );
}

fn container<EVT: EventValueType>(vals: Vec<EVT>) -> ContainerEvents<EVT> {
    let mut evs = ContainerEvents::new();
    for (i, v) in vals.into_iter().enumerate() {
        evs.push_back(TsNano::from_ns(1_700_000_000_000_000_000 + i as u64 * 1_000_000), v);
    }
    evs
}

#[test]
fn cbor_compat_container_events_scalars() {
    check(
        &container(vec![1u8, 2, 255]),
        "a263747373831b17979cfe362a00001b17979cfe363942401b17979cfe364884806476616c7383010218ff",
    );
    check(
        &container(vec![-1i8, 2, -128]),
        "a263747373831b17979cfe362a00001b17979cfe363942401b17979cfe364884806476616c73832002387f",
    );
    check(
        &container(vec![1u16, 2, 65535]),
        "a263747373831b17979cfe362a00001b17979cfe363942401b17979cfe364884806476616c7383010219ffff",
    );
    check(
        &container(vec![-1i16, 2, i16::MIN]),
        "a263747373831b17979cfe362a00001b17979cfe363942401b17979cfe364884806476616c73832002397fff",
    );
    check(
        &container(vec![1u32, 2, u32::MAX]),
        "a263747373831b17979cfe362a00001b17979cfe363942401b17979cfe364884806476616c738301021affffffff",
    );
    check(
        &container(vec![-1i32, 2, i32::MIN]),
        "a263747373831b17979cfe362a00001b17979cfe363942401b17979cfe364884806476616c738320023a7fffffff",
    );
    check(
        &container(vec![1u64, 2, u64::MAX]),
        "a263747373831b17979cfe362a00001b17979cfe363942401b17979cfe364884806476616c738301021bffffffffffffffff",
    );
    check(
        &container(vec![-1i64, 2, i64::MIN]),
        "a263747373831b17979cfe362a00001b17979cfe363942401b17979cfe364884806476616c738320023b7fffffffffffffff",
    );
    check(
        &container(vec![true, false, true]),
        "a263747373831b17979cfe362a00001b17979cfe363942401b17979cfe364884806476616c7383f5f4f5",
    );
    check(
        &container(vec![String::from("a"), String::new(), String::from("äö")]),
        "a263747373831b17979cfe362a00001b17979cfe363942401b17979cfe364884806476616c738361616064c3a4c3b6",
    );
}

#[test]
fn cbor_compat_container_events_floats() {
    check(
        &container(vec![0.0f32, 1.5, -2.25, f32::MAX, f32::MIN_POSITIVE, 0.1]),
        "a263747373861b17979cfe362a00001b17979cfe363942401b17979cfe364884801b17979cfe3657c6c01b17979cfe366709001b17979cfe36764b406476616c7386f90000f93e00f9c080fa7f7ffffffa00800000fa3dcccccd",
    );
    check(
        &container(vec![0.0f64, 1.5, -2.25, f64::MAX, f64::MIN_POSITIVE, 0.1, 1e300]),
        "a263747373871b17979cfe362a00001b17979cfe363942401b17979cfe364884801b17979cfe3657c6c01b17979cfe366709001b17979cfe36764b401b17979cfe36858d806476616c7387f90000f93e00f9c080fb7feffffffffffffffb0010000000000000fb3fb999999999999afb7e37e43c8800759c",
    );
    check(
        &container(vec![f32::INFINITY, f32::NEG_INFINITY]),
        "a263747373821b17979cfe362a00001b17979cfe363942406476616c7382f97c00f9fc00",
    );
    check(
        &container(vec![f64::INFINITY, f64::NEG_INFINITY]),
        "a263747373821b17979cfe362a00001b17979cfe363942406476616c7382f97c00f9fc00",
    );
}

#[test]
fn cbor_compat_container_events_vecs() {
    check(
        &container(vec![vec![1u8, 2, 3], vec![], vec![255]]),
        "a263747373831b17979cfe362a00001b17979cfe363942401b17979cfe364884806476616c738383010203808118ff",
    );
    check(
        &container(vec![vec![1i16, -2], vec![3]]),
        "a263747373821b17979cfe362a00001b17979cfe363942406476616c73828201218103",
    );
    check(
        &container(vec![vec![1u32, 2], vec![]]),
        "a263747373821b17979cfe362a00001b17979cfe363942406476616c738282010280",
    );
    check(
        &container(vec![vec![1.5f32, 2.5], vec![]]),
        "a263747373821b17979cfe362a00001b17979cfe363942406476616c738282f93e00f9410080",
    );
    check(
        &container(vec![vec![1.5f64, 2.5], vec![0.1]]),
        "a263747373821b17979cfe362a00001b17979cfe363942406476616c738282f93e00f9410081fb3fb999999999999a",
    );
    check(
        &container(vec![vec![true, false], vec![]]),
        "a263747373821b17979cfe362a00001b17979cfe363942406476616c738282f5f480",
    );
    check(
        &container(vec![vec![String::from("x"), String::from("yy")], vec![]]),
        "a263747373821b17979cfe362a00001b17979cfe363942406476616c738282617862797980",
    );
}

#[test]
fn cbor_compat_container_events_enum() {
    check(
        &container(vec![EnumVariant::new(0, "a"), EnumVariant::new(-3, "bcd")]),
        "a263747373821b17979cfe362a00001b17979cfe363942406476616c73a263697873820022656e616d657382616163626364",
    );
}

#[test]
fn cbor_compat_container_events_empty() {
    check(&ContainerEvents::<f64>::new(), "a263747373806476616c7380");
}

#[derive(Serialize, Deserialize)]
struct Contents {
    files: HashMap<String, Vec<u8>>,
    dirs: HashMap<String, Contents>,
}

#[test]
fn cbor_compat_contents() {
    let leaf = Contents {
        files: HashMap::from([(String::from("f"), vec![0, 1, 2, 255])]),
        dirs: HashMap::new(),
    };
    let root = Contents {
        files: HashMap::from([(String::from("g"), vec![])]),
        dirs: HashMap::from([(String::from("d"), leaf)]),
    };
    check(
        &root,
        "a26566696c6573a16167806464697273a16164a26566696c6573a161668400010218ff6464697273a0",
    );
}

fn hex_of(v: &cbor2::Value) -> String {
    hex::encode(cbor2::to_vec(v).unwrap())
}

#[test]
fn cbor_compat_value_macros() {
    assert_eq!(
        hex_of(&cbor2::cbor!({ "rangeFinal" => true }).unwrap()),
        "a16a72616e676546696e616cf5"
    );
    assert_eq!(
        hex_of(&cbor2::cbor!({ "error" => "some message" }).unwrap()),
        "a1656572726f726c736f6d65206d657373616765"
    );
    assert_eq!(hex_of(&cbor2::cbor!({ "ERROR" => true }).unwrap()), "a1654552524f52f5");
}

#[test]
fn cbor_compat_value_decode() {
    let bytes = hex::decode("a2656572726f72616d616e03").unwrap();
    let v: cbor2::Value = cbor2::from_slice(&bytes).unwrap();
    let map = v.as_map().unwrap();
    assert_eq!(map.len(), 2);
    assert_eq!(map[0].0.as_text(), Some("error"));
    assert_eq!(map[0].1.as_text(), Some("m"));
}
