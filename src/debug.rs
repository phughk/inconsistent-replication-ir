use std::fmt::Debug;

pub trait MaybeDebug {
    fn maybe_debug(&self) -> String;
}

impl<A: Debug> MaybeDebug for A {
    fn maybe_debug(&self) -> String {
        format!("{:?}", self)
    }
}

impl<A> MaybeDebug for A {
    default fn maybe_debug(&self) -> String {
        "NotDebug".to_string()
    }
}
