
mod metadata;
mod bloom;



pub enum CollisionResult {
    Zero,
    Partial(u32),
    Complete(u32, u32),
    Error,
}

#[derive(Eq, Hash, PartialEq)]
pub enum FilterType {
    Outer,
    Inner,
}