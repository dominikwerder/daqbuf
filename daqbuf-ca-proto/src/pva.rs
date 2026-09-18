pub mod field;
pub mod merge;
pub mod proto;
pub mod pvdata;
pub mod search;
pub mod value;

use std::io;

autoerr::create_error_v1!(
    name(Error, "Pva"),
    enum variants {
        SlideBuf(#[from] slidebuf::Error),
        IO(#[from] io::Error),
        BadSlice,
        BadMagic(u8),
        BadSize,
        BadUtf8,
        NotEnoughInput(usize, usize),
        StringTooLong(usize),
        ArrayTooLong(usize),
        BitSetTooLong(usize),
        PayloadTooLarge(u32),
        BufferTooSmallForNeedMin(usize, usize),
        NoReadBufferSpace,
        NeitherPendingNorProgress,
        OutputBufferTooSmall,
        LogicError,
        ParseAttemptInDoneState,
        BadTypeCode(u8),
        BadScalarTypeCode(u8),
        BadArrayKindForComplexType(u8),
        BoundedStringUnsupported,
        UnknownIntroId(u16),
        NullFieldDescWithId,
        IntroRegistryFull,
        StructTooDeep,
        StructTooManyFields(usize),
        ExpectedStructIntrospection,
        MissingIntrospection,
        NoIntrospectionForRequest(u32),
        BadUnionSelector(usize),
        BadNodeOffset(u32),
        DeltaTypeMismatch,
        SegmentWithoutStart,
        SegmentTooLarge(usize),
        SegmentCommandMismatch(u8, u8),
        MsgNotSerializable,
        TrailingPayload(usize),
        BadRequestString,
    },
);
