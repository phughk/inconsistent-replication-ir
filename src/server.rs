use crate::io::{IRNetwork, IRStorage};
use crate::types::{IRMessage, NodeID};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;

/// Implementation of a server node for receiving and handling operations according to the
/// Inconsistent Replication algorithm.
pub struct InconsistentReplicationServer<
    NET: IRNetwork<ID, MSG>,
    STO: IRStorage<ID, MSG>,
    ID: NodeID,
    MSG: IRMessage,
> {
    network: NET,
    storage: STO,
    node_id: ID,
    view: ViewState,
    _a: PhantomData<MSG>,
}

impl<N, S, I, M> Clone for InconsistentReplicationServer<N, S, I, M>
where
    N: IRNetwork<I, M> + Clone,
    S: IRStorage<I, M> + Clone,
    I: NodeID,
    M: IRMessage,
{
    fn clone(&self) -> Self {
        InconsistentReplicationServer {
            network: self.network.clone(),
            storage: self.storage.clone(),
            node_id: self.node_id.clone(),
            view: self.view.clone(),
            _a: PhantomData,
        }
    }
}

///
#[derive(Clone)]
enum ViewState {
    Normal { view: usize },
    ViewChanging { view: usize },
    Recovery { view: usize },
}

impl<N: IRNetwork<I, M>, S: IRStorage<I, M>, I: NodeID, M: IRMessage>
    InconsistentReplicationServer<N, S, I, M>
{
    pub fn new(network: N, storage: S, node_id: I) -> Self {
        InconsistentReplicationServer {
            network,
            storage,
            node_id,
            view: ViewState::Recovery { view: 0 },
            _a: PhantomData,
        }
    }

    pub fn exec_inconsistent(
        &self,
        client_id: I,
        operation_sequence: usize,
        message: M,
    ) -> Pin<Box<dyn Future<Output = M>>> {
        self.storage
            .record_tentative(client_id, operation_sequence, message)
    }

    pub fn exec_consistent(&self, message: M) -> M {
        unimplemented!("Implement me!");
    }
}

#[cfg(test)]
mod test {
    use crate::io::test_utils::{MockIRNetwork, MockIRStorage};
    use crate::server::InconsistentReplicationServer;
    use std::sync::Arc;

    #[tokio::test]
    pub async fn starts_in_view_zero() {
        // when
        let network = MockIRNetwork::<Arc<String>, Arc<String>, MockIRStorage>::new();
        let storage = MockIRStorage {};

        let server =
            InconsistentReplicationServer::new(network.clone(), storage, Arc::new("1".to_string()));
        network.register_node(Arc::new("1".to_string()), server.clone());
        // server.get_view();
        unimplemented!("Implement me!");
    }
}
