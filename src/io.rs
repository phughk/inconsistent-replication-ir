use crate::types::{IRMessage, NodeID};
use std::future::Future;
use std::pin::Pin;

/// Tracks membership, ID to IP address mapping, and messaging
pub trait IRNetwork<I: NodeID, M: IRMessage> {
    /// Send a heartbeat to a node
    fn heartbeat_node(&self, node_id: I) -> Pin<Box<dyn Future<Output = ()>>>;
    /// Get the current list of members in the view
    fn get_members(&self) -> Pin<Box<dyn Future<Output = Vec<I>>>>;
    /// Used by clients to make an inconsistent request to a specific node
    fn request_inconsistent(
        &self,
        destination: I,
        message: M,
    ) -> Pin<Box<dyn Future<Output = Result<M, ()>>>>;
}

/// Provides access to a storage log for views and persistence
pub trait IRStorage<ID: NodeID, MSG: IRMessage> {
    /// Record a message as tentative for a client and operation number
    fn record_tentative(
        &self,
        client: ID,
        operation: usize,
        message: MSG,
    ) -> Pin<Box<dyn Future<Output = ()>>>;

    /// Promote a tentative record to finalized
    fn promote_finalized(&self, client: ID, operation: usize) -> Pin<Box<dyn Future<Output = ()>>>;
}

/// Provides access to some simple client mechanisms
pub trait ClientStorage {}

#[cfg(test)]
pub mod test_utils {
    use crate::io::{IRNetwork, IRStorage};
    use crate::types::{IRMessage, NodeID};
    use std::future::Future;

    pub struct MockIRNetwork {}

    impl<I: NodeID, M: IRMessage> IRNetwork<I, M> for MockIRNetwork {
        fn heartbeat_node(&self, node_id: I) -> Box<dyn Future<Output = ()>> {
            todo!()
        }

        fn get_members(&self) -> Box<dyn Future<Output = Vec<I>>> {
            todo!()
        }

        fn request_inconsistent(
            &self,
            destination: I,
            message: M,
        ) -> Box<dyn Future<Output = Result<M, ()>>> {
            todo!()
        }
    }

    pub struct MockIRStorage {}

    impl<ID: NodeID, MSG: IRMessage> IRStorage<ID, MSG> for MockIRStorage {
        fn record_tentative(
            &self,
            client: ID,
            operation: usize,
            message: MSG,
        ) -> Box<dyn Future<Output = ()>> {
            todo!()
        }

        fn promote_finalized(&self, client: ID, operation: usize) -> Box<dyn Future<Output = ()>> {
            todo!()
        }
    }
}
