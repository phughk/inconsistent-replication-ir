use std::ops::Add;
use std::sync::Arc;

pub trait NodeID: Clone + PartialEq + Ord + PartialOrd {}

impl<A> NodeID for A where A: Clone + PartialEq + Ord + PartialOrd {}

pub trait IRMessage: Clone + PartialEq + PartialOrd {}

impl<A> IRMessage for A where A: Clone + PartialEq + PartialOrd {}

pub trait Incrementable {
    fn increment(&self) -> Self;
}

impl<A> Incrementable for A
where
    A: Clone + Add<usize, Output = A>,
{
    fn increment(&self) -> Self {
        self.clone() + 1
    }
}
