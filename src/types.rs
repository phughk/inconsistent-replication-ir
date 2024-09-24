pub trait NodeID: Clone + PartialEq + Ord + PartialOrd {}

impl<A> NodeID for A where A: Clone + PartialEq + Ord + PartialOrd {}

pub trait IRMessage: Clone + PartialEq + PartialOrd {}

impl<A> IRMessage for A where A: Clone + PartialEq + PartialOrd {}
