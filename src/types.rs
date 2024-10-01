pub trait NodeID: Clone + PartialEq + Ord + PartialOrd + 'static {}

impl<A> NodeID for A where A: Clone + PartialEq + Ord + PartialOrd + 'static {}

pub trait IRMessage: Clone + PartialEq + Ord + PartialOrd + 'static {}

impl<A> IRMessage for A where A: Clone + PartialEq + Ord + PartialOrd + 'static {}

pub trait DecideFunction<M: IRMessage> {
    fn decide<'a, S: IntoIterator<Item = &'a M>>(&self, choices: S) -> &'a M;
}

pub type OperationSequence = u64;
