#[allow(non_snake_case)]
pub mod serde_Instant_elapsed_ms {
    use serde::Serializer;
    use std::time::Instant;

    #[allow(unused)]
    pub fn serialize<S>(val: &Instant, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let dur = val.elapsed();
        ser.serialize_u64(dur.as_secs() * 1000 + dur.subsec_millis() as u64)
    }
}

#[allow(non_snake_case)]
pub mod serde_Instant_as_system_time {
    use serde::Serializer;
    use std::time::Instant;

    #[allow(unused)]
    pub fn serialize<S>(val: &Instant, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let st = time::UtcDateTime::now();
        let dur = val.elapsed();
        let st2 = st
            .checked_sub(time::SignedDuration::new(dur.as_secs() as _, dur.subsec_nanos() as _))
            .unwrap_or(st);
        // ser.serialize_u64(dur.as_secs() * 1000 + dur.subsec_millis() as u64)
        let fmt = time::format_description::parse_borrowed::<3>(
            "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:3]",
        )
        .unwrap();
        let v = st2.format(&fmt).unwrap_or(String::new());
        ser.serialize_str(&v)
    }
}
