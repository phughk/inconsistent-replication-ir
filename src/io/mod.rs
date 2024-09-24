#[cfg(test)]
pub mod test_utils;

use crate::types::{IRMessage, NodeID};
use std::future::Future;
use std::pin::Pin;

/// Tracks membership, ID to IP address mapping, and messaging
pub trait IRNetwork<I: NodeID, M: IRMessage> {
    /// Get the current list of members in the view
    fn get_members(&self) -> Pin<Box<dyn Future<Output = Vec<I>>>>;

    /// Used by clients to make an inconsistent request to a specific node
    fn request_inconsistent(
        &self,
        destination: I,
        client_id: I,
        sequence: usize,
        message: M,
    ) -> Pin<Box<dyn Future<Output = Result<M, ()>>>>;
}

/// Provides access to a storage log for views and persistence
pub trait IRStorage<ID: NodeID, MSG: IRMessage> {
    /// Record a message as tentative for a client and operation number
    /// The message must be recorded as tentative even if the operation is rejected
    /// This is to resolve quorums
    fn record_tentative(
        &self,
        client: ID,
        operation: usize,
        message: MSG,
    ) -> Pin<Box<dyn Future<Output = MSG>>>;

    /// Promote a tentative record to finalized
    fn promote_finalized(&self, client: ID, operation: usize) -> Pin<Box<dyn Future<Output = ()>>>;
}
