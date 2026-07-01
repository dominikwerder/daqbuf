#[test]
fn test_json_trailing() {
    use serde::Deserialize;
    use serde_json::Value as JsonValue;
    use std::io::Cursor;
    if serde_json::from_str::<JsonValue>(r#"{}."#).is_ok() {
        panic!("Should fail because of trailing character");
    }
    let cur = Cursor::new(r#"{}..."#);
    let mut de = serde_json::Deserializer::from_reader(cur);
    if JsonValue::deserialize(&mut de).is_err() {
        panic!("Should allow trailing characters")
    }
    let cur = Cursor::new(r#"nullA"#);
    let mut de = serde_json::Deserializer::from_reader(cur);
    if let Ok(val) = JsonValue::deserialize(&mut de) {
        if val != serde_json::json!(null) {
            panic!("Bad parse")
        }
    } else {
        panic!("Should allow trailing characters")
    }
    let cur = Cursor::new(r#"  {}AA"#);
    let mut de = serde_json::Deserializer::from_reader(cur);
    if let Ok(val) = JsonValue::deserialize(&mut de) {
        if val != serde_json::json!({}) {
            panic!("Bad parse")
        }
    } else {
        panic!("Should allow trailing characters")
    }
}
