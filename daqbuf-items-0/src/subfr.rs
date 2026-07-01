use crate::streamitem::CONTAINER_EVENTS_TYPE_ID;
use netpod::EnumVariant;

type SubIdTy = u16;

pub const VEC_FLAG: SubIdTy = 0x0400;
pub const PULSED_FLAG: SubIdTy = 0x0800;

pub trait SubFrId {
    const SUB: SubIdTy;
}

impl SubFrId for u8 {
    const SUB: SubIdTy = 0x01;
}

impl SubFrId for u16 {
    const SUB: SubIdTy = 0x02;
}

impl SubFrId for u32 {
    const SUB: SubIdTy = 0x03;
}

impl SubFrId for u64 {
    const SUB: SubIdTy = 0x04;
}

impl SubFrId for i8 {
    const SUB: SubIdTy = 0x05;
}

impl SubFrId for i16 {
    const SUB: SubIdTy = 0x06;
}

impl SubFrId for i32 {
    const SUB: SubIdTy = 0x07;
}

impl SubFrId for i64 {
    const SUB: SubIdTy = 0x08;
}

impl SubFrId for f32 {
    const SUB: SubIdTy = 0x09;
}

impl SubFrId for f64 {
    const SUB: SubIdTy = 0x0a;
}

impl SubFrId for bool {
    const SUB: SubIdTy = 0x0b;
}

impl SubFrId for String {
    const SUB: SubIdTy = 0x0c;
}

impl SubFrId for EnumVariant {
    const SUB: SubIdTy = 0x0d;
}

impl SubFrId for netpod::UnsupEvt {
    const SUB: SubIdTy = 0x0e;
}

impl SubFrId for Vec<u8> {
    const SUB: SubIdTy = VEC_FLAG | <u8 as SubFrId>::SUB;
}

impl SubFrId for Vec<u16> {
    const SUB: SubIdTy = VEC_FLAG | <u16 as SubFrId>::SUB;
}

impl SubFrId for Vec<u32> {
    const SUB: SubIdTy = VEC_FLAG | <u32 as SubFrId>::SUB;
}

impl SubFrId for Vec<u64> {
    const SUB: SubIdTy = VEC_FLAG | <u64 as SubFrId>::SUB;
}

impl SubFrId for Vec<i8> {
    const SUB: SubIdTy = VEC_FLAG | <i8 as SubFrId>::SUB;
}

impl SubFrId for Vec<i16> {
    const SUB: SubIdTy = VEC_FLAG | <i16 as SubFrId>::SUB;
}

impl SubFrId for Vec<i32> {
    const SUB: SubIdTy = VEC_FLAG | <i32 as SubFrId>::SUB;
}

impl SubFrId for Vec<i64> {
    const SUB: SubIdTy = VEC_FLAG | <i64 as SubFrId>::SUB;
}

impl SubFrId for Vec<f32> {
    const SUB: SubIdTy = VEC_FLAG | <f32 as SubFrId>::SUB;
}

impl SubFrId for Vec<f64> {
    const SUB: SubIdTy = VEC_FLAG | <f64 as SubFrId>::SUB;
}

impl SubFrId for Vec<bool> {
    const SUB: SubIdTy = VEC_FLAG | <bool as SubFrId>::SUB;
}

impl SubFrId for Vec<String> {
    const SUB: SubIdTy = VEC_FLAG | <String as SubFrId>::SUB;
}

impl SubFrId for Vec<EnumVariant> {
    const SUB: SubIdTy = VEC_FLAG | <EnumVariant as SubFrId>::SUB;
}

impl SubFrId for Vec<netpod::UnsupEvt> {
    const SUB: SubIdTy = VEC_FLAG | <netpod::UnsupEvt as SubFrId>::SUB;
}

pub const fn is_vec_subfr(x: SubIdTy) -> bool {
    x & VEC_FLAG != 0
}

pub const fn pulsed_subfr(x: SubIdTy) -> SubIdTy {
    PULSED_FLAG | x
}

pub const fn is_pulsed_subfr(x: SubIdTy) -> bool {
    x & PULSED_FLAG != 0
}

pub const fn subfr_scalar_type(x: SubIdTy) -> SubIdTy {
    x & 0xff
}

pub const fn is_container_events(x: u32) -> bool {
    x & 0xffff0000 == CONTAINER_EVENTS_TYPE_ID
}
