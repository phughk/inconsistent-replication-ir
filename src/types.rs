use std::sync::Arc;

pub trait NodeID: Clone + PartialEq + Ord + PartialOrd {}

impl NodeID for &'static str {}
impl NodeID for Arc<String> {}

pub trait IRMessage: Clone + PartialEq + PartialOrd {}

impl IRMessage for Arc<String> {}
