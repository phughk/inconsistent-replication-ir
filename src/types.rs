use std::fmt::Debug;

/// The representation of a node id in a cluster, or a client id
/// This requires the Debug trait since it is used in errors
pub trait NodeID: Clone + PartialEq + Ord + PartialOrd + Debug + 'static {}

impl<A> NodeID for A where A: Clone + PartialEq + Ord + PartialOrd + Debug + 'static {}

pub trait IRMessage: Clone + PartialEq + Ord + PartialOrd + 'static {}

impl<A> IRMessage for A where A: Clone + PartialEq + Ord + PartialOrd + 'static {}

pub trait DecideFunction<M: IRMessage> {
    fn decide<'a, S: IntoIterator<Item = &'a M>>(&self, choices: S) -> &'a M;
}

pub type OperationSequence = u64;
