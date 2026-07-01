#[allow(non_snake_case)]
pub mod serde_Duration_human {
    use serde::Serializer;
    use std::time::Duration;

    #[allow(unused)]
    pub fn serialize<S>(val: &Duration, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let secs = val.as_secs();
        let ms = val.subsec_millis();
        if secs > 190 {
            let mins = secs / 60;
            let secs = secs % 60;
            ser.serialize_str(&format!("{mins}m{secs}s{ms}ms"))
        } else {
            ser.serialize_str(&format!("{secs}s{ms}ms"))
        }
    }
}
