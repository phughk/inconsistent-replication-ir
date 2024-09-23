use crate::types::{IRMessage, NodeID};
use std::future::Future;

/// Tracks membership, ID to IP address mapping, and messaging
pub trait IRNetwork<I: NodeID, M: IRMessage> {
    /// Send a heartbeat to a node
    fn heartbeat_node(&self, node_id: I) -> Box<dyn Future<Output = ()>>;
    /// Get the current list of members in the view
    fn get_members(&self) -> Box<dyn Future<Output = Vec<I>>>;
    /// Used by clients to make an inconsistent request to a specific node
    fn request_inconsistent(
        &self,
        destination: I,
        message: M,
    ) -> Box<dyn Future<Output = Result<M, ()>>>;
}

/// Provides access to a storage log for views and persistence
pub trait IRStorage {}

/// Provides access to some simple client mechanisms
pub trait ClientStorage {}

#[cfg(test)]
pub mod test_utils {
    use crate::io::{IRNetwork, IRStorage};
    use crate::types::NodeID;
    use std::future::Future;

    pub struct MockIRNetwork {}

    impl<I: NodeID> IRNetwork<I> for MockIRNetwork {
        fn heartbeat_node(&self, node_id: I) -> Box<dyn Future<Output = ()>> {
            todo!()
        }

        fn get_members(&self) -> Box<dyn Future<Output = Vec<I>>> {
            todo!()
        }

        fn request_inconsistent(&self) -> Box<dyn Future<Output = ()>> {
            todo!()
        }
    }

    pub struct MockIRStorage {}

    impl IRStorage for MockIRStorage {}
}
