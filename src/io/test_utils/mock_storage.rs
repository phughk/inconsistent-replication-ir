use crate::types::{IRMessage, NodeID};
use crate::IRStorage;
use std::future::Future;
use std::pin::Pin;

#[derive(Clone)]
pub struct MockIRStorage {}

impl<ID: NodeID, MSG: IRMessage> IRStorage<ID, MSG> for MockIRStorage {
    fn record_tentative(
        &self,
        client: ID,
        operation: usize,
        message: MSG,
    ) -> Pin<Box<dyn Future<Output = ()>>> {
        todo!()
    }

    fn promote_finalized(&self, client: ID, operation: usize) -> Pin<Box<dyn Future<Output = ()>>> {
        todo!()
    }
}
