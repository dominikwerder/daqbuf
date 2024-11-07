// Required for any inner type of Sitemty.
pub trait FrameTypeInnerStatic {
    const FRAME_TYPE_ID: u32;
}

// To be implemented by the T of Sitemty<T>, e.g. ScalarEvents.
pub trait FrameTypeInnerDyn {
    // TODO check actual usage of this
    fn frame_type_id(&self) -> u32;
}

impl<T> FrameTypeInnerDyn for T
where
    T: FrameTypeInnerStatic,
{
    fn frame_type_id(&self) -> u32 {
        <Self as FrameTypeInnerStatic>::FRAME_TYPE_ID
    }
}
