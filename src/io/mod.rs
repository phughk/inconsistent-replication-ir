#[cfg(any(test, feature = "test"))]
pub mod test_utils;

use crate::server::View;
use crate::types::{IRMessage, NodeID};
use std::future::Future;
use std::pin::Pin;

/// Tracks membership, ID to IP address mapping, and messaging
pub trait IRNetwork<I: NodeID, M: IRMessage> {

    /// Used by clients to make an inconsistent request to a specific node
    fn propose_inconsistent(
        &self,
        destination: I,
        client_id: I,
        sequence: u64,
        message: M,
        highest_observed_view: Option<View<I>>,
    ) -> Pin<Box<dyn Future<Output = Result<(M, View<I>), ()>>>>;

    /// Used by clients to make a consistent request to a specific node
    fn propose_consistent(
        &self,
        destination: I,
        client_id: I,
        sequence: u64,
        message: M,
    ) -> Pin<Box<dyn Future<Output = Result<(M, View<I>), ()>>>>;

    /// Send a finalize message to a node
    /// This does not need to be immediate, for example it can be buffered and sent
    /// together with another message
    fn async_finalize(
        &self,
        destination: I,
        client_id: I,
        sequence: u64,
        message: M,
    ) -> Pin<Box<dyn Future<Output = Result<(), ()>>>>;

    /// Send a finalize message to a node
    /// This *DOES* need to be immediate, though can be batched.
    fn sync_finalize(
        &self,
        destination: I,
        client_id: I,
        sequence: u64,
        message: M,
    ) -> Pin<Box<dyn Future<Output = Result<(M, View<I>), ()>>>>;

    /// A client that detects a higher view will notify a node to change view
    fn invoke_view_change(
        &self,
        destination: I,
        view: View<I>,
    ) -> Pin<Box<dyn Future<Output = Result<View<I>, ()>>>>;
}

/// Provides access to a storage log for views and persistence
pub trait IRStorage<ID: NodeID, MSG: IRMessage> {
    /// Record a message as tentative for a client and operation number
    /// The message must be recorded as tentative even if the operation is rejected
    /// This is to resolve quorums
    fn record_tentative(
        &self,
        client: ID,
        operation: u64,
        message: MSG,
    ) -> Pin<Box<dyn Future<Output = MSG>>>;

    /// Promote a tentative operation to finalized and execute it
    fn promote_finalized_and_run(
        &self,
        client: ID,
        operation: u64,
    ) -> Pin<Box<dyn Future<Output = ()>>>;

    /// Used by clients and servers to recover the current view, thus obtaining members
    fn recover_current_view(&self) -> Pin<Box<dyn Future<Output = View<ID>>>>;
}
