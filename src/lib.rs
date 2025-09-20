
mod metadata;
mod bloom;



pub enum CollisionResult {
    Zero,
    PartialMinor(u32),
    PartialMajor(u32, u32),
    Complete(u32, u32, u32),
    Error,
}

pub enum FilterType {
    Outer,
    Inner,
}