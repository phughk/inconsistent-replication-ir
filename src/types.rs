pub trait NodeID: Clone + PartialEq + Ord + PartialOrd {}

impl<A> NodeID for A where A: Clone + PartialEq + Ord + PartialOrd {}

pub trait IRMessage: Clone + PartialEq + Ord + PartialOrd {}

impl<A> IRMessage for A where A: Clone + PartialEq + Ord + PartialOrd {}

pub trait DecideFunction<M: IRMessage> {
    fn decide<'a, S: IntoIterator<Item = &'a M>>(&self, choices: S) -> &'a M;
}
