use netpod::EnumVariant;

pub trait SubFrId {
    const SUB: u32;
}

impl SubFrId for u8 {
    const SUB: u32 = 0x03;
}

impl SubFrId for u16 {
    const SUB: u32 = 0x05;
}

impl SubFrId for u32 {
    const SUB: u32 = 0x08;
}

impl SubFrId for u64 {
    const SUB: u32 = 0x0a;
}

impl SubFrId for i8 {
    const SUB: u32 = 0x02;
}

impl SubFrId for i16 {
    const SUB: u32 = 0x04;
}

impl SubFrId for i32 {
    const SUB: u32 = 0x07;
}

impl SubFrId for i64 {
    const SUB: u32 = 0x09;
}

impl SubFrId for f32 {
    const SUB: u32 = 0x0b;
}

impl SubFrId for f64 {
    const SUB: u32 = 0x0c;
}

impl SubFrId for bool {
    const SUB: u32 = 0x0d;
}

impl SubFrId for String {
    const SUB: u32 = 0x0e;
}

impl SubFrId for EnumVariant {
    const SUB: u32 = 0x0f;
}

impl SubFrId for Vec<u8> {
    const SUB: u32 = 0x23;
}

impl SubFrId for Vec<u16> {
    const SUB: u32 = 0x25;
}

impl SubFrId for Vec<u32> {
    const SUB: u32 = 0x28;
}

impl SubFrId for Vec<u64> {
    const SUB: u32 = 0x2a;
}

impl SubFrId for Vec<i8> {
    const SUB: u32 = 0x22;
}

impl SubFrId for Vec<i16> {
    const SUB: u32 = 0x24;
}

impl SubFrId for Vec<i32> {
    const SUB: u32 = 0x27;
}

impl SubFrId for Vec<i64> {
    const SUB: u32 = 0x29;
}

impl SubFrId for Vec<f32> {
    const SUB: u32 = 0x2b;
}

impl SubFrId for Vec<f64> {
    const SUB: u32 = 0x2c;
}

impl SubFrId for Vec<bool> {
    const SUB: u32 = 0x2d;
}

impl SubFrId for Vec<String> {
    const SUB: u32 = 0x2e;
}

impl SubFrId for Vec<EnumVariant> {
    const SUB: u32 = 0x2f;
}
